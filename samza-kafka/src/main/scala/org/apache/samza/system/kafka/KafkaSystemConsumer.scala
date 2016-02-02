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

import kafka.common.TopicAndPartition
import org.apache.samza.util.Logging
import kafka.message.Message
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
import org.apache.samza.util.TopicMetadataStore
import kafka.api.TopicMetadata
import org.apache.samza.util.ExponentialSleepStrategy
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemAdmin

object KafkaSystemConsumer {

  // Approximate additional shallow heap overhead per message in addition to the raw bytes
  // received from Kafka  4 + 64 + 4 + 4 + 4 = 80 bytes overhead.
  // As this overhead is a moving target, and not very large
  // compared to the message size its being ignore in the computation for now.
  val MESSAGE_SIZE_OVERHEAD =  4 + 64 + 4 + 4 + 4;

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
  systemAdmin: SystemAdmin,
  metrics: KafkaSystemConsumerMetrics,
  metadataStore: TopicMetadataStore,
  clientId: String = "undefined-client-id-%s" format UUID.randomUUID.toString,
  timeout: Int = ConsumerConfig.ConsumerTimeoutMs,
  bufferSize: Int = ConsumerConfig.SocketBufferSize,
  fetchSize: StreamFetchSizes = new StreamFetchSizes,
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
  /**
   * Defines a low water mark for how many bytes we buffer before we start
   * executing fetch requests against brokers to get more messages. This
   * value is divided by 2 because the messages are buffered twice, once in
   * KafkaConsumer and then in SystemConsumers. This value
   * is divided equally among all registered SystemStreamPartitions.
   * However this is a soft limit per partition, as the
   * bytes are cached at the message boundaries, and the actual usage can be
   * 1000 bytes + size of max message in the partition for a given stream.
   * The bytes if the size of the bytebuffer in Message. Hence, the
   * Object overhead is not taken into consideration. In this codebase
   * it seems to be quite small. Hence, even for 500000 messages this is around 4MB x 2 = 8MB,
   * which is not considerable.
   *
   * For example,
   * if fetchThresholdBytes is set to 100000 bytes, and there are 50
   * SystemStreamPartitions registered, then the per-partition threshold is
   * (100000 / 2) / 50 = 1000 bytes.
   * As this is a soft limit, the actual usage can be 1000 bytes + size of max message.
   * As soon as a SystemStreamPartition's buffered messages bytes drops
   * below 1000, a fetch request will be executed to get more data for it.
   *
   * Increasing this parameter will decrease the latency between when a queue
   * is drained of messages and when new messages are enqueued, but also leads
   * to an increase in memory usage since more messages will be held in memory.
   *
   * The default value is -1, which means this is not used. When the value
   * is > 0, then the fetchThreshold which is count based is ignored.
   */
  fetchThresholdBytes: Long = -1,
  /**
   * if(fetchThresholdBytes > 0) true else false
   */
  fetchLimitByBytesEnabled: Boolean = false,
  offsetGetter: GetOffset = new GetOffset("fail"),
  deserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  keyDeserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
  retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
  clock: () => Long = { System.currentTimeMillis }) extends BlockingEnvelopeMap(
    metrics.registry,
    new Clock {
      def currentTimeMillis = clock()
    },
    classOf[KafkaSystemConsumerMetrics].getName,
    fetchLimitByBytesEnabled) with Toss with Logging {

  type HostPort = (String, Int)
  val brokerProxies = scala.collection.mutable.Map[HostPort, BrokerProxy]()
  val topicPartitionsAndOffsets: scala.collection.concurrent.Map[TopicAndPartition, String] = new ConcurrentHashMap[TopicAndPartition, String]()
  var perPartitionFetchThreshold = fetchThreshold
  var perPartitionFetchThresholdBytes = 0L

  def start() {
    if (topicPartitionsAndOffsets.size > 0) {
      perPartitionFetchThreshold = fetchThreshold / topicPartitionsAndOffsets.size
      // messages get double buffered, hence divide by 2
      if(fetchLimitByBytesEnabled) {
        perPartitionFetchThresholdBytes = (fetchThresholdBytes / 2) / topicPartitionsAndOffsets.size
      }
    }

    refreshBrokers
  }

  override def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    super.register(systemStreamPartition, offset)

    val topicAndPartition = KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition)
    val existingOffset = topicPartitionsAndOffsets.getOrElseUpdate(topicAndPartition, offset)
    // register the older offset in the consumer
    if (systemAdmin.offsetComparator(existingOffset, offset) >= 0) {
      topicPartitionsAndOffsets.replace(topicAndPartition, offset)
    }

    metrics.registerTopicAndPartition(KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition))
  }

  def stop() {
    brokerProxies.values.foreach(_.stop)
  }

  protected def createBrokerProxy(host: String, port: Int): BrokerProxy = {
    new BrokerProxy(host, port, systemName, clientId, metrics, sink, timeout, bufferSize, fetchSize, consumerMinSize, consumerMaxWait, offsetGetter)
  }

  protected def getHostPort(topicMetadata: TopicMetadata, partition: Int): Option[(String, Int)] = {
    // Whatever we do, we can't say Broker, even though we're
    // manipulating it here. Broker is a private type and Scala doesn't seem
    // to care about that as long as you don't explicitly declare its type.
    val brokerOption = topicMetadata
      .partitionsMetadata
      .find(_.partitionId == partition)
      .flatMap(_.leader)

    brokerOption match {
      case Some(broker) => Some(broker.host, broker.port)
      case _ => None
    }
  }

  def refreshBrokers {
    var tpToRefresh = topicPartitionsAndOffsets.keySet.toList
    info("Refreshing brokers for: %s" format topicPartitionsAndOffsets)
    retryBackoff.run(
      loop => {
        val topics = tpToRefresh.map(_.topic).toSet
        val topicMetadata = TopicMetadataCache.getTopicMetadata(topics, systemName, (topics: Set[String]) => metadataStore.getTopicInfo(topics))

        // addTopicPartition one at a time, leaving the to-be-done list intact in case of exceptions.
        // This avoids trying to re-add the same topic partition repeatedly
        def refresh(tp: List[TopicAndPartition]) = {
          val head :: rest = tpToRefresh
          // refreshBrokers can be called from abdicate and refreshDropped, 
          // both of which are triggered from BrokerProxy threads. To prevent 
          // accidentally creating multiple objects for the same broker, or 
          // accidentally not updating the topicPartitionsAndOffsets variable, 
          // we need to lock. 
          this.synchronized {
            // Check if we still need this TopicAndPartition inside the 
            // critical section. If we don't, then skip it.
            topicPartitionsAndOffsets.get(head) match {
              case Some(nextOffset) =>
                getHostPort(topicMetadata(head.topic), head.partition) match {
                  case Some((host, port)) =>
                    val brokerProxy = brokerProxies.getOrElseUpdate((host, port), createBrokerProxy(host, port))
                    brokerProxy.addTopicPartition(head, Option(nextOffset))
                    brokerProxy.start
                    debug("Claimed topic-partition (%s) for (%s)".format(head, brokerProxy))
                    topicPartitionsAndOffsets -= head
                  case None => info("No metadata available for: %s. Will try to refresh and add to a consumer thread later." format head)
                }
              case _ => debug("Ignoring refresh for %s because we already added it from another thread." format head)
            }
          }
          rest
        }

        while (!tpToRefresh.isEmpty) {
          tpToRefresh = refresh(tpToRefresh)
        }

        loop.done
      },

      (exception, loop) => {
        warn("While refreshing brokers for %s: %s. Retrying." format (tpToRefresh.head, exception))
        debug("Exception detail:", exception)
      })
  }

  val sink = new MessageSink {
    var lastDroppedRefresh = clock()

    def refreshDropped() {
      if (topicPartitionsAndOffsets.size > 0 && clock() - lastDroppedRefresh > 10000) {
        refreshBrokers
        lastDroppedRefresh = clock()
      }
    }

    def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      setIsAtHead(toSystemStreamPartition(tp), isAtHighWatermark)
    }

    def needsMoreMessages(tp: TopicAndPartition) = {
      if(fetchLimitByBytesEnabled) {
        getMessagesSizeInQueue(toSystemStreamPartition(tp)) < perPartitionFetchThresholdBytes
      } else {
        getNumMessagesInQueue(toSystemStreamPartition(tp)) < perPartitionFetchThreshold
      }
    }

    def getMessageSize(message: Message): Integer = {
      message.size + KafkaSystemConsumer.MESSAGE_SIZE_OVERHEAD
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

      if(fetchLimitByBytesEnabled ) {
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message, getMessageSize(msg.message)))
      } else {
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message))
      }

      setIsAtHead(systemStreamPartition, isAtHead)
    }

    def abdicate(tp: TopicAndPartition, nextOffset: Long) {
      info("Abdicating for %s" format (tp))
      topicPartitionsAndOffsets += tp -> nextOffset.toString
      refreshBrokers
    }

    private def toSystemStreamPartition(tp: TopicAndPartition) = {
      new SystemStreamPartition(systemName, tp.topic, new Partition(tp.partition))
    }
  }
}
