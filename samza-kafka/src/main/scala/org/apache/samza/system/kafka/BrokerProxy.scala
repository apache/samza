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

import java.nio.channels.ClosedByInterruptException
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import kafka.api._
import kafka.common.{NotLeaderForPartitionException, UnknownTopicOrPartitionException, ErrorMapping, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.message.MessageSet
import org.apache.samza.SamzaException
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.Logging
import org.apache.samza.util.ThreadNamePrefix.SAMZA_THREAD_NAME_PREFIX
import scala.collection.JavaConversions._
import scala.collection.concurrent
import scala.collection.mutable
import org.apache.samza.util.KafkaUtil

/**
 *  Companion object for class JvmMetrics encapsulating various constants
 */
object BrokerProxy {
  val BROKER_PROXY_THREAD_NAME_PREFIX = "BROKER-PROXY-"
}

/**
 * A BrokerProxy consolidates Kafka fetches meant for a particular broker and retrieves them all at once, providing
 * a way for consumers to retrieve those messages by topic and partition.
 */
class BrokerProxy(
  val host: String,
  val port: Int,
  val system: String,
  val clientID: String,
  val metrics: KafkaSystemConsumerMetrics,
  val messageSink: MessageSink,
  val timeout: Int = ConsumerConfig.SocketTimeout,
  val bufferSize: Int = ConsumerConfig.SocketBufferSize,
  val fetchSize: StreamFetchSizes = new StreamFetchSizes,
  val consumerMinSize:Int = ConsumerConfig.MinFetchBytes,
  val consumerMaxWait:Int = ConsumerConfig.MaxFetchWaitMs,
  offsetGetter: GetOffset = new GetOffset("fail")) extends Toss with Logging {

  /**
   * How long should the fetcher thread sleep before checking if any TopicPartitions has been added to its purview
   */
  val sleepMSWhileNoTopicPartitions = 100

  /** What's the next offset for a particular partition? **/
  val nextOffsets:concurrent.Map[TopicAndPartition, Long] = new ConcurrentHashMap[TopicAndPartition, Long]()

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

    val sc = new DefaultFetchSimpleConsumer(host, port, timeout, bufferSize, clientID, fetchSize, consumerMinSize, consumerMaxWait)
    sc
  }

  def addTopicPartition(tp: TopicAndPartition, nextOffset: Option[String]) = {
    debug("Adding new topic and partition %s to queue for %s" format (tp, host))

    if (nextOffsets.containsKey(tp)) {
      toss("Already consuming TopicPartition %s" format tp)
    }

    val offset = if (nextOffset.isDefined && offsetGetter.isValidOffset(simpleConsumer, tp, nextOffset.get)) {
      nextOffset
        .get
        .toLong
    } else {
      warn("It appears that we received an invalid or empty offset %s for %s. Attempting to use Kafka's auto.offset.reset setting. This can result in data loss if processing continues." format (nextOffset, tp))

      offsetGetter.getResetOffset(simpleConsumer, tp)
    }

    debug("Got offset %s for new topic and partition %s." format (offset, tp))

    nextOffsets += tp -> offset

    metrics.topicPartitions(host, port).set(nextOffsets.size)
  }

  def removeTopicPartition(tp: TopicAndPartition) = {
    if (nextOffsets.containsKey(tp)) {
      val offset = nextOffsets.remove(tp)
      metrics.topicPartitions(host, port).set(nextOffsets.size)
      debug("Removed %s" format tp)
      offset
    } else {
      warn("Asked to remove topic and partition %s, but not in map (keys = %s)" format (tp, nextOffsets.keys.mkString(",")))
      None
    }
  }

  val thread = new Thread(new Runnable {
    def run {
      var reconnect = false

      try {
        (new ExponentialSleepStrategy).run(
          loop => {
            if (reconnect) {
              metrics.reconnects(host, port).inc
              simpleConsumer.close()
              simpleConsumer = createSimpleConsumer()
            }

            while (!Thread.currentThread.isInterrupted) {
              messageSink.refreshDropped
              if (nextOffsets.size == 0) {
                debug("No TopicPartitions to fetch. Sleeping.")
                Thread.sleep(sleepMSWhileNoTopicPartitions)
              } else {
                fetchMessages

                // If we got here, fetchMessages didn't throw an exception, i.e. it was successful.
                // In that case, reset the loop delay, so that the next time an error occurs,
                // we start with a short retry delay.
                loop.reset
              }
            }
          },

          (exception, loop) => {
            warn("Restarting consumer due to %s. Releasing ownership of all partitions, and restarting consumer. Turn on debugging to get a full stack trace." format exception)
            debug("Exception detail:", exception)
            abdicateAll
            reconnect = true
          })
      } catch {
        case e: InterruptedException       => info("Got interrupt exception in broker proxy thread.")
        case e: ClosedByInterruptException => info("Got closed by interrupt exception in broker proxy thread.")
        case e: OutOfMemoryError           => throw new SamzaException("Got out of memory error in broker proxy thread.")
        case e: StackOverflowError         => throw new SamzaException("Got stack overflow error in broker proxy thread.")
      }

      if (Thread.currentThread.isInterrupted) info("Shutting down due to interrupt.")
    }
  }, "BrokerProxy thread pointed at %s:%d for client %s" format (host, port, clientID))

  private def fetchMessages(): Unit = {
    val topicAndPartitionsToFetch = nextOffsets.filterKeys(messageSink.needsMoreMessages(_)).toList

    if (topicAndPartitionsToFetch.size > 0) {
      metrics.brokerReads(host, port).inc
      val response: FetchResponse = simpleConsumer.defaultFetch(topicAndPartitionsToFetch: _*)
      firstCall = false
      firstCallBarrier.countDown()

      // Split response into errors and non errors, processing the errors first
      val (nonErrorResponses, errorResponses) = response.data.entrySet().partition(_.getValue.error == ErrorMapping.NoError)

      handleErrors(errorResponses, response)

      nonErrorResponses.foreach { nonError => moveMessagesToTheirQueue(nonError.getKey, nonError.getValue) }
    } else {
      refreshLatencyMetrics

      debug("No topic/partitions need to be fetched for %s:%s right now. Sleeping %sms." format (host, port, sleepMSWhileNoTopicPartitions))

      metrics.brokerSkippedFetchRequests(host, port).inc

      Thread.sleep(sleepMSWhileNoTopicPartitions)
    }
  }

  /**
   * Releases ownership for a single TopicAndPartition. The 
   * KafkaSystemConsumer will try and find a new broker for the 
   * TopicAndPartition.
   */
  def abdicate(tp: TopicAndPartition) = removeTopicPartition(tp) match {
    // Need to be mindful of a tp that was removed by another thread
    case Some(offset) => messageSink.abdicate(tp, offset)
    case None => warn("Tried to abdicate for topic partition not in map. Removed in interim?")
  }

  /**
   * Releases all TopicAndPartition ownership for this BrokerProxy thread. The 
   * KafkaSystemConsumer will try and find a new broker for the 
   * TopicAndPartition.
   */
  def abdicateAll {
    nextOffsets.keySet.foreach(abdicate(_))
  }

  def handleErrors(errorResponses: mutable.Set[Entry[TopicAndPartition, FetchResponsePartitionData]], response:FetchResponse) = {
    // FetchResponse should really return Option and a list of the errors so we don't have to find them ourselves
    case class Error(tp: TopicAndPartition, code: Short, exception: Throwable)

    // Now subdivide the errors into three types: non-recoverable, not leader (== abdicate) and offset out of range (== get new offset)

    // Convert FetchResponse into easier-to-work-with Errors
    val errors = for (
      error <- errorResponses;
      errorCode <- Option(response.errorCode(error.getKey.topic, error.getKey.partition)); // Scala's being cranky about referring to error.getKey values...
      exception <- Option(ErrorMapping.exceptionFor(errorCode))
    ) yield new Error(error.getKey, errorCode, exception)

    val (notLeaderOrUnknownTopic, otherErrors) = errors.partition { case (e) => e.code == ErrorMapping.NotLeaderForPartitionCode || e.code == ErrorMapping.UnknownTopicOrPartitionCode }
    val (offsetOutOfRangeErrors, remainingErrors) = otherErrors.partition(_.code == ErrorMapping.OffsetOutOfRangeCode)

    // Can recover from two types of errors: not leader (go find the new leader) and offset out of range (go get the new offset)
    // However, we want to bail as quickly as possible if there are non recoverable errors so that the state of the other
    // topic-partitions remains the same.  That way, when we've rebuilt the simple consumer, we can come around and
    // handle the recoverable errors.
    remainingErrors.foreach(e => {
      warn("Got non-recoverable error codes during multifetch. Throwing an exception to trigger reconnect. Errors: %s" format remainingErrors.mkString(","))
      KafkaUtil.maybeThrowException(e.code) })

    notLeaderOrUnknownTopic.foreach(e => abdicate(e.tp))

    offsetOutOfRangeErrors.foreach(e => {
      warn("Received OffsetOutOfRange exception for %s. Current offset = %s" format (e.tp, nextOffsets.getOrElse(e.tp, "not found in map, likely removed in the interim")))

      try {
        val newOffset = offsetGetter.getResetOffset(simpleConsumer, e.tp)
        // Put the new offset into the map (if the tp still exists).  Will catch it on the next go-around
        nextOffsets.replace(e.tp, newOffset)
      } catch {
        // UnknownTopic or NotLeader are routine events and handled via abdication.  All others, bail.
        case _ @ (_:UnknownTopicOrPartitionException | _: NotLeaderForPartitionException) => warn("Received (UnknownTopicOr|NotLeaderFor)Partition exception. Abdicating")
                                                                                             abdicate(e.tp)
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
      metrics.highWatermark(tp).set(hw)
      metrics.lag(tp).set(hw - nextOffset)
    } else {
      debug("Got a high water mark less than 0 (%d) for %s, so skipping." format (hw, tp))
    }
  }
  override def toString() = "BrokerProxy for %s:%d" format (host, port)

  def start {
    if (!thread.isAlive) {
      info("Starting " + toString)
      thread.setDaemon(true)
      thread.setName(SAMZA_THREAD_NAME_PREFIX + BrokerProxy.BROKER_PROXY_THREAD_NAME_PREFIX + thread.getName)
      thread.start
    } else {
      debug("Tried to start an already started broker proxy (%s). Ignoring." format toString)
    }
  }

  def stop {
    info("Shutting down " + toString)

    if (simpleConsumer != null) {
      info("closing simple consumer...")
      simpleConsumer.close
    }

    thread.interrupt
    thread.join
  }

  private def refreshLatencyMetrics {
    nextOffsets.foreach{
      case (topicAndPartition, offset) => {
        val latestOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, -1, Request.OrdinaryConsumerId)
        trace("latest offset of %s is %s" format (topicAndPartition, latestOffset))
        if (latestOffset >= 0) {
          // only update the registered topicAndpartitions
          if(metrics.highWatermark.containsKey(topicAndPartition)) {
            metrics.highWatermark(topicAndPartition).set(latestOffset)
          }
          if(metrics.lag.containsKey(topicAndPartition)) {
            metrics.lag(topicAndPartition).set(latestOffset - offset)
          }
        }
      }
    }
  }
}