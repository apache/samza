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

import kafka.api._
import kafka.common.{NotLeaderForPartitionException, UnknownTopicOrPartitionException, ErrorMapping, TopicAndPartition}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import scala.collection.JavaConversions._
import kafka.message.MessageSet
import grizzled.slf4j.Logging
import java.nio.channels.ClosedByInterruptException
import java.util.Map.Entry
import scala.collection.mutable

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
  val nextOffsets:mutable.ConcurrentMap[TopicAndPartition, Long] = new ConcurrentHashMap[TopicAndPartition, Long]()

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

    val sc = new DefaultFetchSimpleConsumer(host, port, timeout, bufferSize, clientID) {
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
      warn("Asked to remove topic and partition %s, but not in map (keys = %s)" format (tp, nextOffsets.keys.mkString(",")))
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
              warn("Shutting down due to interrupt exception.")
              Thread.currentThread.interrupt
            case e: ClosedByInterruptException =>
              warn("Shutting down due to closed by interrupt exception.")
              Thread.currentThread.interrupt
            case e: Throwable => {
              warn("Recreating simple consumer and retrying connection")
              warn("Stack trace for fetchMessages exception.", e)
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

    // Split response into errors and non errors, processing the errors first
    val (nonErrorResponses, errorResponses) = response.data.entrySet().partition(_.getValue.error == ErrorMapping.NoError)

    handleErrors(errorResponses, response)

    nonErrorResponses.foreach { nonError => moveMessagesToTheirQueue(nonError.getKey, nonError.getValue) }
  }

  def handleErrors(errorResponses: mutable.Set[Entry[TopicAndPartition, FetchResponsePartitionData]], response:FetchResponse) = {
    // Need to be mindful of a tp that was removed by another thread
    def abdicate(tp:TopicAndPartition) = nextOffsets.remove(tp) match {
        case Some(offset) => messageSink.abdicate(tp, offset -1)
        case None         => warn("Tried to abdicate for topic partition not in map. Removed in interim?")
      }

    // FetchResponse should really return Option and a list of the errors so we don't have to find them ourselves
    case class Error(tp: TopicAndPartition, code: Short, exception: Throwable)

    // Now subdivide the errors into three types: non-recoverable, not leader (== abdicate) and offset out of range (== get new offset)

    // Convert FetchResponse into easier-to-work-with Errors
    val errors = for (
      error <- errorResponses;
      errorCode <- Option(response.errorCode(error.getKey.topic, error.getKey.partition)); // Scala's being cranky about referring to error.getKey values...
      exception <- Option(ErrorMapping.exceptionFor(errorCode))
    ) yield new Error(error.getKey, errorCode, exception)

    val (notLeaders, otherErrors) = errors.partition(_.code == ErrorMapping.NotLeaderForPartitionCode)
    val (offsetOutOfRangeErrors, remainingErrors) = otherErrors.partition(_.code == ErrorMapping.OffsetOutOfRangeCode)

    // Can recover from two types of errors: not leader (go find the new leader) and offset out of range (go get the new offset)
    // However, we want to bail as quickly as possible if there are non recoverable errors so that the state of the other
    // topic-partitions remains the same.  That way, when we've rebuilt the simple consumer, we can come around and
    // handle the recoverable errors.
    remainingErrors.foreach(e => {
      warn("Got non-recoverable error codes during multifetch. Throwing an exception to trigger reconnect. Errors: %s" format remainingErrors.mkString(","))
      ErrorMapping.maybeThrowException(e.code) })

    // Go back one message, since the fetch for nextOffset failed, and
    // abdicate requires lastOffset, not nextOffset.
    notLeaders.foreach(e => abdicate(e.tp))

    offsetOutOfRangeErrors.foreach(e => {
      warn("Received OffsetOutOfRange exception for %s. Current offset = %s" format (e.tp, nextOffsets.getOrElse(e.tp, "not found in map, likely removed in the interim")))

      try {
        val newOffset = offsetGetter.getNextOffset(simpleConsumer, e.tp, null)
        // Put the new offset into the map (if the tp still exists).  Will catch it on the next go-around
        nextOffsets.replace(e.tp, newOffset)
      } catch {
        // UnknownTopic or NotLeader are routine events and handled via abdication.  All others, bail.
        case _ @ (_:UnknownTopicOrPartitionException | _: NotLeaderForPartitionException) => warn("Received (UnknownTopicOr|NotLeaderFor)Partition exception. Abdicating")
                                                                                             abdicate(e.tp)
        case other => throw other
      }
    })
  }

  def moveMessagesToTheirQueue(tp: TopicAndPartition, data: FetchResponsePartitionData) = {
    val messageSet: MessageSet = data.messages
    var nextOffset = nextOffsets(tp)

    messageSink.setIsAtHighWatermark(tp, data.hw == 0 || data.hw == nextOffset)
    require(messageSet != null)
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
  override def toString() = "BrokerProxy for %s:%d" format (host, port)

  def start {
    info("Starting " + toString)

    thread.setDaemon(true)
    thread.start
  }

  def stop {
    info("Shutting down " + toString)

    thread.interrupt
    thread.join
  }
}
