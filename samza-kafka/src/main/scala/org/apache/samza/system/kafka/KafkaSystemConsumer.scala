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

import org.apache.samza.util.ClientUtilTopicMetadataStore
import kafka.common.TopicAndPartition
import grizzled.slf4j.Logging
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
  clientId: String = "undefined-client-id-%s" format UUID.randomUUID.toString,
  timeout: Int = Int.MaxValue,
  bufferSize: Int = 1024000,
  brokerMetadataFailureRefreshMs: Long = 10000,
  fetchThreshold: Int = 0,
  offsetGetter: GetOffset = new GetOffset("fail"),
  deserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  keyDeserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  clock: () => Long = { System.currentTimeMillis }) extends BlockingEnvelopeMap(metrics.registry, new Clock {
  def currentTimeMillis = clock()
}, classOf[KafkaSystemConsumerMetrics].getName) with Toss with Logging {

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
    var tpToRefresh = topicPartitionsAndOffsets.keySet.toList
    while (!tpToRefresh.isEmpty) {
      try {
        val getTopicMetadata = (topics: Set[String]) => {
          new ClientUtilTopicMetadataStore(brokerListString, clientId).getTopicInfo(topics)
        }
        val topics = tpToRefresh.map(_.topic).toSet
        val partitionMetadata = TopicMetadataCache.getTopicMetadata(topics, systemName, getTopicMetadata)

        // addTopicPartition one at a time, leaving the to-be-done list intact in case of exceptions.
        // This avoids trying to re-add the same topic partition repeatedly
        def refresh(tp:List[TopicAndPartition]) = {
          val head :: rest = tpToRefresh
          val lastOffset = topicPartitionsAndOffsets.get(head).get
          // Whatever we do, we can't say Broker, even though we're
          // manipulating it here. Broker is a private type and Scala doesn't seem
          // to care about that as long as you don't explicitly declare its type.
          val brokerOption = partitionMetadata(head.topic)
                             .partitionsMetadata
                             .find(_.partitionId == head.partition)
                             .flatMap(_.leader)

          brokerOption match {
            case Some(broker) =>
              val brokerProxy = brokerProxies.getOrElseUpdate((broker.host, broker.port), new BrokerProxy(broker.host, broker.port, systemName, clientId, metrics, timeout, bufferSize, offsetGetter) {
                val messageSink: MessageSink = sink
              })

              brokerProxy.addTopicPartition(head, Option(lastOffset))
            case None => warn("No such topic-partition: %s, dropping." format head)
          }
          rest
        }


        while(!tpToRefresh.isEmpty) {
          tpToRefresh = refresh(tpToRefresh)
        }
      } catch {
        case e: Throwable =>
          warn("An exception was thrown while refreshing brokers for %s. Waiting a bit and retrying, since we can't continue without broker metadata." format tpToRefresh.head)
          debug("Exception while refreshing brokers", e)

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

      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message))

      setIsAtHead(systemStreamPartition, isAtHead)
    }

    def abdicate(tp: TopicAndPartition, lastOffset: Long) {
      info("Abdicating for %s" format (tp))
      refreshBrokers(Map(tp -> lastOffset.toString))
    }

    private def toSystemStreamPartition(tp: TopicAndPartition) = {
      new SystemStreamPartition(systemName, tp.topic, new Partition(tp.partition))
    }
  }
}
