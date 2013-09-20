/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import kafka.consumer.SimpleConsumer
import kafka.api._
import kafka.common.ErrorMapping
import java.util.concurrent.{ CountDownLatch, ConcurrentHashMap }
import scala.collection.JavaConversions._
import org.apache.samza.config.Config
import org.apache.samza.util.KafkaUtil
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.metrics.MetricsRegistry
import kafka.common.TopicAndPartition
import kafka.message.MessageSet
import grizzled.slf4j.Logging
import java.nio.channels.ClosedByInterruptException

/**
 * A BrokerProxy consolidates Kafka fetches meant for a particular broker and retrieves them all at once, providing
 * a way for consumers to retrieve those messages by topic and partition.
 */
abstract class BrokerProxy(
  val host: String,
  val port: Int,
  val system: String,
  val clientID: String,
  val metrics: KafkaSystemConsumerMetrics,
  val timeout: Int = Int.MaxValue,
  val bufferSize: Int = 1024000,
  offsetGetter: GetOffset = new GetOffset("fail")) extends Toss with Logging {

  val messageSink: MessageSink

  /**
   * How long should the fetcher thread sleep before checking if any TopicPartitions has been added to its purview
   */
  val sleepMSWhileNoTopicPartitions = 1000

  /** What's the next offset for a particular partition? **/
  val nextOffsets: ConcurrentHashMap[TopicAndPartition, Long] = new ConcurrentHashMap[TopicAndPartition, Long]()

  /** Block on the first call to get message if the fetcher has not yet returned its initial results **/
  // TODO: It should be sufficient to just use the count down latch and await on it for each of the calls, but
  // VisualVM was showing the consumer thread spending all its time in the await method rather than returning
  // immediately, even though the process was proceeding normally.  Hence the extra boolean.  Should be investigated.
  val firstCallBarrier = new CountDownLatch(1)
  var firstCall = true

  var simpleConsumer = createSimpleConsumer()

  metrics.registerBrokerProxy(host, port)

  def createSimpleConsumer() = {
    val hostString = "%s:%d" format (host, port)
    info("Creating new SimpleConsumer for host %s for system %s" format (hostString, system))

    val sc = new SimpleConsumer(host, port, timeout, bufferSize, clientID) with DefaultFetch {
      val fetchSize: Int = 256 * 1024
    }

    sc
  }

  def addTopicPartition(tp: TopicAndPartition, lastCheckpointedOffset: String) = {
    debug("Adding new topic and partition %s to queue for %s" format (tp, host))
    if (nextOffsets.containsKey(tp)) toss("Already consuming TopicPartition %s" format tp)

    val offset = offsetGetter.getNextOffset(simpleConsumer, tp, lastCheckpointedOffset)
    nextOffsets += tp -> offset

    metrics.topicPartitions(host, port).set(nextOffsets.size)
  }

  def removeTopicPartition(tp: TopicAndPartition) = {
    if (nextOffsets.containsKey(tp)) {
      nextOffsets.remove(tp)
      metrics.topicPartitions(host, port).set(nextOffsets.size)
      debug("Removed %s" format tp)
    } else {
      warn("Asked to remove topic and partition %s, but not in map (keys = %s)" format (tp, nextOffsets.keys().mkString(",")))
    }
  }

  val thread: Thread = new Thread(new Runnable() {
    def run() {
      info("Starting thread for BrokerProxy")

      while (!Thread.currentThread.isInterrupted) {
        if (nextOffsets.size == 0) {
          debug("No TopicPartitions to fetch. Sleeping.")
          Thread.sleep(sleepMSWhileNoTopicPartitions)
        } else {
          try {
            fetchMessages()
          } catch {
            // If we're interrupted, don't try and reconnect. We should shut down.
            case e: InterruptedException =>
              debug("Shutting down due to interrupt exception.")
              Thread.currentThread.interrupt
            case e: ClosedByInterruptException =>
              debug("Shutting down due to closed by interrupt exception.")
              Thread.currentThread.interrupt
            case e: Throwable => {
              warn("Recreating simple consumer and retrying connection")
              debug("Stack trace for fetchMessages exception.", e)
              simpleConsumer.close()
              simpleConsumer = createSimpleConsumer()
              metrics.reconnects(host, port).inc
            }
          }
        }
      }

    }
  }, "BrokerProxy thread pointed at %s:%d for client %s" format (host, port, clientID))

  private def fetchMessages(): Unit = {
    metrics.brokerReads(host, port).inc
    val response: FetchResponse = simpleConsumer.defaultFetch(nextOffsets.filterKeys(messageSink.needsMoreMessages(_)).toList: _*)
    firstCall = false
    firstCallBarrier.countDown()
    if (response.hasError) {
      // FetchResponse should really return Option and a list of the errors so we don't have to find them ourselves
      case class Error(tp: TopicAndPartition, code: Short, exception: Throwable)

      val errors = for (
        error <- response.data.entrySet.filter(_.getValue.error != ErrorMapping.NoError);
        errorCode <- Option(response.errorCode(error.getKey.topic, error.getKey.partition)); // Scala's being cranky about referring to error.getKey values...
        exception <- Option(ErrorMapping.exceptionFor(errorCode))
      ) yield new Error(error.getKey, errorCode, exception)

      val (notLeaders, otherErrors) = errors.partition(_.code == ErrorMapping.NotLeaderForPartitionCode)

      if (!notLeaders.isEmpty) {
        info("Abdicating. Got not leader exception for: " + notLeaders.mkString(","))

        notLeaders.foreach(e => {
          // Go back one message, since the fetch for nextOffset failed, and 
          // abdicate requires lastOffset, not nextOffset.
          messageSink.abdicate(e.tp, nextOffsets.remove(e.tp) - 1)
        })
      }

      if (!otherErrors.isEmpty) {
        warn("Got error codes during multifetch. Throwing an exception to trigger reconnect. Errors: %s" format errors.mkString(","))
        otherErrors.foreach(e => ErrorMapping.maybeThrowException(e.code)) // One will get thrown
      }
    }

    def moveMessagesToTheirQueue(tp: TopicAndPartition, data: FetchResponsePartitionData) = {
      val messageSet: MessageSet = data.messages
      var nextOffset = nextOffsets(tp)

      messageSink.setIsAtHighWatermark(tp, data.hw == 0 || data.hw == nextOffset)

      for (message <- messageSet.iterator) {
        messageSink.addMessage(tp, message, data.hw) // TODO: Verify this is correct

        nextOffset = message.nextOffset

        val bytesSize = message.message.payloadSize + message.message.keySize
        metrics.reads(tp).inc
        metrics.bytesRead(tp).inc(bytesSize)
        metrics.brokerBytesRead(host, port).inc(bytesSize)
        metrics.offsets(tp).set(nextOffset)
      }

      nextOffsets.replace(tp, nextOffset) // use replace rather than put in case this tp was removed while we were fetching.

      // Update high water mark
      val hw = data.hw
      if (hw >= 0) {
        metrics.lag(tp).set(hw - nextOffset)
      } else {
        debug("Got a high water mark less than 0 (%d) for %s, so skipping." format (hw, tp))
      }
    }

    response.data.foreach { case (tp, data) => moveMessagesToTheirQueue(tp, data) }

  }

  override def toString() = "BrokerProxy for %s:%d" format (host, port)

  def start {
    debug("Starting broker proxy for %s:%s." format (host, port))

    thread.setDaemon(true)
    thread.start
  }

  def stop {
    debug("Shutting down broker proxy for %s:%s." format (host, port))

    thread.interrupt
    thread.join
  }
}
