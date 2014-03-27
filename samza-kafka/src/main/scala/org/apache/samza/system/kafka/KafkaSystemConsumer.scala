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
import kafka.consumer.ConsumerConfig
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.SamzaException

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
  timeout: Int = ConsumerConfig.ConsumerTimeoutMs,
  bufferSize: Int = ConsumerConfig.SocketBufferSize,
  fetchSize: Int = ConsumerConfig.MaxFetchSize,
  consumerMinSize: Int = ConsumerConfig.MinFetchBytes,
  consumerMaxWait: Int = ConsumerConfig.MaxFetchWaitMs,

  /**
   * Defines a low water mark for how many messages we buffer before we start
   * executing fetch requests against brokers to get more messages. This value
   * is divided equally among all registered SystemStreamPartitions. For
   * example, if fetchThreshold is set to 50000, and there are 50
   * SystemStreamPartitions registered, then the per-partition threshold is
   * 1000. As soon as a SystemStreamPartition's buffered message count drops
   * below 1000, a fetch request will be executed to get more data for it.
   *
   * Increasing this parameter will decrease the latency between when a queue
   * is drained of messages and when new messages are enqueued, but also leads
   * to an increase in memory usage since more messages will be held in memory.
   */
  fetchThreshold: Int = 50000,
  offsetGetter: GetOffset = new GetOffset("fail"),
  deserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  keyDeserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
  clock: () => Long = { System.currentTimeMillis }) extends BlockingEnvelopeMap(metrics.registry, new Clock {
  def currentTimeMillis = clock()
}, classOf[KafkaSystemConsumerMetrics].getName) with Toss with Logging {

  type HostPort = (String, Int)
  val brokerProxies = scala.collection.mutable.Map[HostPort, BrokerProxy]()
  var nextOffsets = Map[SystemStreamPartition, String]()
  var perPartitionFetchThreshold = fetchThreshold

  def start() {
    if (nextOffsets.size > 0) {
      perPartitionFetchThreshold = fetchThreshold / nextOffsets.size
    }

    val topicPartitionsAndOffsets = nextOffsets.map {
      case (systemStreamPartition, offset) =>
        val topicAndPartition = KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition)
        (topicAndPartition, offset)
    }

    refreshBrokers(topicPartitionsAndOffsets)

    brokerProxies.values.foreach(_.start)
  }

  override def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    super.register(systemStreamPartition, offset)

    nextOffsets += systemStreamPartition -> offset

    metrics.registerTopicAndPartition(KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition))
  }

  def stop() {
    brokerProxies.values.foreach(_.stop)
  }

  def refreshBrokers(topicPartitionsAndOffsets: Map[TopicAndPartition, String]) {
    var tpToRefresh = topicPartitionsAndOffsets.keySet.toList
    retryBackoff.run(
      loop => {
        val getTopicMetadata = (topics: Set[String]) => {
          new ClientUtilTopicMetadataStore(brokerListString, clientId, timeout).getTopicInfo(topics)
        }
        val topics = tpToRefresh.map(_.topic).toSet
        val partitionMetadata = TopicMetadataCache.getTopicMetadata(topics, systemName, getTopicMetadata)

        // addTopicPartition one at a time, leaving the to-be-done list intact in case of exceptions.
        // This avoids trying to re-add the same topic partition repeatedly
        def refresh(tp: List[TopicAndPartition]) = {
          val head :: rest = tpToRefresh
          val nextOffset = topicPartitionsAndOffsets.get(head).get
          // Whatever we do, we can't say Broker, even though we're
          // manipulating it here. Broker is a private type and Scala doesn't seem
          // to care about that as long as you don't explicitly declare its type.
          val brokerOption = partitionMetadata(head.topic)
            .partitionsMetadata
            .find(_.partitionId == head.partition)
            .flatMap(_.leader)

          brokerOption match {
            case Some(broker) =>
              def createBrokerProxy = new BrokerProxy(broker.host, broker.port, systemName, clientId, metrics, sink, timeout, bufferSize, fetchSize, consumerMinSize, consumerMaxWait, offsetGetter)

              brokerProxies
                .getOrElseUpdate((broker.host, broker.port), createBrokerProxy)
                .addTopicPartition(head, Option(nextOffset))
            case None => warn("No such topic-partition: %s, dropping." format head)
          }
          rest
        }

        while (!tpToRefresh.isEmpty) {
          tpToRefresh = refresh(tpToRefresh)
        }
        loop.done
      },

      (loop, exception) => {
        warn("While refreshing brokers for %s: %s. Retrying." format (tpToRefresh.head, exception))
        debug(exception)
      })
  }

  val sink = new MessageSink {
    def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      setIsAtHead(toSystemStreamPartition(tp), isAtHighWatermark)
    }

    def needsMoreMessages(tp: TopicAndPartition) = {
      getNumMessagesInQueue(toSystemStreamPartition(tp)) <= perPartitionFetchThreshold
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
      val message = if (!msg.message.isNull) {
        deserializer.fromBytes(Utils.readBytes(msg.message.payload))
      } else {
        null
      }

      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message))

      setIsAtHead(systemStreamPartition, isAtHead)
    }

    def abdicate(tp: TopicAndPartition, nextOffset: Long) {
      info("Abdicating for %s" format (tp))
      refreshBrokers(Map(tp -> nextOffset.toString))
    }

    private def toSystemStreamPartition(tp: TopicAndPartition) = {
      new SystemStreamPartition(systemName, tp.topic, new Partition(tp.partition))
    }
  }
}
