/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.kafka

import org.apache.samza.util.{ KafkaUtil, ClientUtilTopicMetadataStore }
import kafka.common.TopicAndPartition
import org.apache.samza.config.{ KafkaConfig, Config }
import org.apache.samza.SamzaException
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.metrics.MetricsRegistry
import grizzled.slf4j.Logging
import scala.collection.JavaConversions._
import kafka.message.MessageAndOffset
import org.apache.samza.Partition
import kafka.utils.Utils
import org.apache.samza.util.Clock
import java.util.UUID
import kafka.serializer.DefaultDecoder
import kafka.serializer.Decoder
import org.apache.samza.util.BlockingEnvelopeMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope
import java.nio.charset.Charset

object KafkaSystemConsumer {
  def toTopicAndPartition(systemStreamPartition: SystemStreamPartition) = {
    val topic = systemStreamPartition.getStream
    val partitionId = systemStreamPartition.getPartition.getPartitionId
    TopicAndPartition(topic, partitionId)
  }
}

/**
 *  Maintain a cache of BrokerProxies, returning the appropriate one for the
 *  requested topic and partition.
 */
private[kafka] class KafkaSystemConsumer(
  systemName: String,
  brokerListString: String,
  metrics: KafkaSystemConsumerMetrics,
  clientId: String = "undefined-client-id-" + UUID.randomUUID.toString,
  timeout: Int = Int.MaxValue,
  bufferSize: Int = 1024000,
  brokerMetadataFailureRefreshMs: Long = 10000,
  fetchThreshold: Int = 0,
  offsetGetter: GetOffset = new GetOffset("fail"),
  deserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  keyDeserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  clock: () => Long = { System.currentTimeMillis }) extends BlockingEnvelopeMap(metrics.registry, new Clock {
  def currentTimeMillis = clock()
}) with Toss with Logging {

  type HostPort = (String, Int)
  val brokerProxies = scala.collection.mutable.Map[HostPort, BrokerProxy]()
  var lastReadOffsets = Map[SystemStreamPartition, String]()

  def start() {
    val topicPartitionsAndOffsets = lastReadOffsets.map {
      case (systemStreamPartition, offset) =>
        val topicAndPartition = KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition)
        (topicAndPartition, offset)
    }

    refreshBrokers(topicPartitionsAndOffsets)

    brokerProxies.values.foreach(_.start)
  }

  override def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) {
    super.register(systemStreamPartition, lastReadOffset)

    lastReadOffsets += systemStreamPartition -> lastReadOffset

    metrics.registerTopicAndPartition(KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition))
  }

  def stop() {
    brokerProxies.values.foreach(_.stop)
  }

  def refreshBrokers(topicPartitionsAndOffsets: Map[TopicAndPartition, String]) {
    var done = false

    while (!done) {
      try {
        val getTopicMetadata = (topics: Set[String]) => {
          new ClientUtilTopicMetadataStore(brokerListString, clientId).getTopicInfo(topics)
        }

        val partitionMetadata = TopicMetadataCache.getTopicMetadata(
          topicPartitionsAndOffsets.keys.map(_.topic).toSet,
          systemName,
          getTopicMetadata)

        topicPartitionsAndOffsets.map {
          case (topicAndPartition, lastOffset) =>
            // TODO whatever we do, we can't say Broker, even though we're 
            // manipulating it here. Broker is a private type and Scala doesn't seem 
            // to care about that as long as you don't explicitly declare its type.
            val brokerOption = partitionMetadata(topicAndPartition.topic)
              .partitionsMetadata
              .find(_.partitionId == topicAndPartition.partition)
              .getOrElse(toss("Can't find leader for %s" format topicAndPartition))
              .leader

            brokerOption match {
              case Some(broker) =>
                val brokerProxy = brokerProxies.getOrElseUpdate((broker.host, broker.port), new BrokerProxy(broker.host, broker.port, systemName, clientId, metrics, timeout, bufferSize, offsetGetter) {
                  val messageSink: MessageSink = sink
                })

                brokerProxy.addTopicPartition(topicAndPartition, lastOffset)
              case _ => warn("Broker for %s not defined! " format topicAndPartition)
            }
        }

        done = true
      } catch {
        case e: Throwable =>
          warn("An exception was thrown while refreshing brokers for %s. Waiting a bit and retrying, since we can't continue without broker metadata." format topicPartitionsAndOffsets.keySet)
          debug(e)

          try {
            Thread.sleep(brokerMetadataFailureRefreshMs)
          } catch {
            case e: InterruptedException =>
              info("Interrupted while waiting to retry metadata refresh, so shutting down.")

              stop
          }
      }
    }
  }

  val sink = new MessageSink {
    def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      setIsAtHead(toSystemStreamPartition(tp), isAtHighWatermark)
    }

    def needsMoreMessages(tp: TopicAndPartition) = {
      getNumMessagesInQueue(toSystemStreamPartition(tp)) <= fetchThreshold
    }

    def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) = {
      trace("Incoming message %s: %s." format (tp, msg))

      val systemStreamPartition = toSystemStreamPartition(tp)
      val isAtHead = highWatermark == msg.offset
      val offset = msg.offset.toString
      val key = if (msg.message.key != null) {
        keyDeserializer.fromBytes(Utils.readBytes(msg.message.key))
      } else {
        null
      }
      val message = if (msg.message.buffer != null) {
        deserializer.fromBytes(Utils.readBytes(msg.message.payload))
      } else {
        null
      }

      add(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message))

      setIsAtHead(systemStreamPartition, isAtHead)
    }

    def abdicate(tp: TopicAndPartition, lastOffset: Long) {
      refreshBrokers(Map(tp -> lastOffset.toString))
    }

    private def toSystemStreamPartition(tp: TopicAndPartition) = {
      new SystemStreamPartition(systemName, tp.topic, new Partition(tp.partition))
    }
  }
}
