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

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import kafka.api.{PartitionOffsetsResponse, _}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{ByteBufferMessageSet, Message, MessageAndOffset, MessageSet}
import org.apache.samza.SamzaException
import org.apache.samza.util.Logging
import org.junit.Assert._
import org.junit._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Matchers, Mockito}

import scala.collection.JavaConversions._

class TestBrokerProxy extends Logging {
  val tp2 = new TopicAndPartition("Redbird", 2013)
  var fetchTp1 = true // control whether fetching tp1 messages or not

  @Test def brokerProxyRetrievesMessagesCorrectly() = {
    val (bp, tp, sink) = getMockBrokerProxy()

    bp.start
    bp.addTopicPartition(tp, Option("0"))
    // Add tp2, which should never receive messages since sink disables it.
    bp.addTopicPartition(tp2, Option("0"))
    Thread.sleep(1000)
    assertEquals(2, sink.receivedMessages.size)
    assertEquals(42, sink.receivedMessages.get(0)._2.offset)
    assertEquals(84, sink.receivedMessages.get(1)._2.offset)
  }

  @Test def brokerProxySkipsFetchForEmptyRequests() = {
    val (bp, tp, sink) = getMockBrokerProxy()

    bp.start
    // Only add tp2, which should never receive messages since sink disables it.
    bp.addTopicPartition(tp2, Option("0"))
    Thread.sleep(1000)
    assertEquals(0, sink.receivedMessages.size)
    assertTrue(bp.metrics.brokerSkippedFetchRequests(bp.host, bp.port).getCount > 0)
    assertEquals(0, bp.metrics.brokerReads(bp.host, bp.port).getCount)
  }

  @Test def brokerProxyThrowsExceptionOnDuplicateTopicPartitions() = {
    val (bp, tp, _) = getMockBrokerProxy()
    bp.start
    bp.addTopicPartition(tp, Option("0"))

    try {
      bp.addTopicPartition(tp, Option("1"))
      fail("Should have thrown an exception")
    } catch {
      case se: SamzaException => assertEquals(se.getMessage, "Already consuming TopicPartition [Redbird,2012]")
      case other: Exception => fail("Got some other exception than what we were expecting: " + other)
    }
  }

  def getMockBrokerProxy() = {
    val sink = new MessageSink {
      val receivedMessages = new scala.collection.mutable.ListBuffer[(TopicAndPartition, MessageAndOffset, Boolean)]()

      def abdicate(tp: TopicAndPartition, nextOffset: Long) {}

      def refreshDropped() {}

      def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) {
        receivedMessages.add((tp, msg, msg.offset.equals(highWatermark)))
      }

      def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      }

      // Never need messages for tp2.
      def needsMoreMessages(tp: TopicAndPartition): Boolean = !tp.equals(tp2) && fetchTp1
    }

    val system = "daSystem"
    val host = "host"
    val port = 2222
    val tp = new TopicAndPartition("Redbird", 2012)
    val metrics = new KafkaSystemConsumerMetrics(system)

    metrics.registerBrokerProxy(host, port)
    metrics.registerTopicAndPartition(tp)
    metrics.topicPartitions(host, port).set(1)

    val bp = new BrokerProxy(
      host,
      port,
      system,
      "daClientId",
      metrics,
      sink,
      offsetGetter = new GetOffset("fail", Map("Redbird" -> "largest"))) {

      override val sleepMSWhileNoTopicPartitions = 100
      // Speed up for test
      var alreadyCreatedConsumer = false

      // Scala traits and Mockito mocks don't mix, unfortunately.
      override def createSimpleConsumer() = {
        if (alreadyCreatedConsumer) {
          System.err.println("Should only be creating one consumer in this test!")
          throw new InterruptedException("Should only be creating one consumer in this test!")
        }
        alreadyCreatedConsumer = true

        new DefaultFetchSimpleConsumer("a", 1, 2, 3, "b", new StreamFetchSizes(42)) {
          val sc = Mockito.mock(classOf[SimpleConsumer])
          val mockOffsetResponse = {
            val offsetResponse = Mockito.mock(classOf[OffsetResponse])
            val partitionOffsetResponse = {
              val por = Mockito.mock(classOf[PartitionOffsetsResponse])
              when(por.offsets).thenReturn(List(1l).toSeq)
              por
            }

            val map = scala.Predef.Map[TopicAndPartition, PartitionOffsetsResponse](tp -> partitionOffsetResponse, tp2 -> partitionOffsetResponse)
            when(offsetResponse.partitionErrorAndOffsets).thenReturn(map)
            offsetResponse
          }

          when(sc.getOffsetsBefore(any(classOf[OffsetRequest]))).thenReturn(mockOffsetResponse)

          val fetchResponse = {
            val fetchResponse = Mockito.mock(classOf[FetchResponse])

            val messageSet = {
              val messageSet = Mockito.mock(classOf[ByteBufferMessageSet])

              def getMessage() = new Message(Mockito.mock(classOf[ByteBuffer]))
              val messages = List(new MessageAndOffset(getMessage, 42), new MessageAndOffset(getMessage, 84))

              when(messageSet.sizeInBytes).thenReturn(43)
              when(messageSet.size).thenReturn(44)
              when(messageSet.iterator).thenReturn(messages.iterator)
              when(messageSet.head).thenReturn(messages.head)
              messageSet
            }

            val fetchResponsePartitionData = FetchResponsePartitionData(0, 500, messageSet)
            val map = scala.Predef.Map[TopicAndPartition, FetchResponsePartitionData](tp -> fetchResponsePartitionData)

            when(fetchResponse.data).thenReturn(map)
            when(fetchResponse.messageSet(any(classOf[String]), any(classOf[Int]))).thenReturn(messageSet)
            fetchResponse
          }
          when(sc.fetch(any(classOf[FetchRequest]))).thenReturn(fetchResponse)

          override def close() = sc.close()

          override def send(request: TopicMetadataRequest): TopicMetadataResponse = sc.send(request)

          override def fetch(request: FetchRequest): FetchResponse = {
            // Verify that we only get fetch requests for one tp, even though
            // two were registered. This is to verify that
            // sink.needsMoreMessages works.
            assertEquals(1, request.requestInfo.size)
            sc.fetch(request)
          }

          when(sc.earliestOrLatestOffset(any(classOf[TopicAndPartition]), any(classOf[Long]), any(classOf[Int]))).thenReturn(100)

          override def getOffsetsBefore(request: OffsetRequest): OffsetResponse = sc.getOffsetsBefore(request)

          override def commitOffsets(request: OffsetCommitRequest): OffsetCommitResponse = sc.commitOffsets(request)

          override def fetchOffsets(request: OffsetFetchRequest): OffsetFetchResponse = sc.fetchOffsets(request)

          override def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = sc.earliestOrLatestOffset(topicAndPartition, earliestOrLatest, consumerId)
        }
      }

    }

    (bp, tp, sink)
  }

  @Test def brokerProxyUpdateLatencyMetrics() = {
    val (bp, tp, _) = getMockBrokerProxy()

    bp.start
    bp.addTopicPartition(tp, Option("0"))
    Thread.sleep(1000)
    // update when fetching messages
    assertEquals(500, bp.metrics.highWatermark(tp).getValue)
    assertEquals(415, bp.metrics.lag(tp).getValue)

    fetchTp1 = false
    Thread.sleep(1000)
    // update when not fetching messages
    assertEquals(100, bp.metrics.highWatermark(tp).getValue)
    assertEquals(15, bp.metrics.lag(tp).getValue)

    fetchTp1 = true
  }

 @Test def brokerProxyCorrectlyHandlesOffsetOutOfRange(): Unit = {
    // Need to wait for the thread to do some work before ending the test
    val countdownLatch = new CountDownLatch(1)
    var failString: String = null

    val mockMessageSink = mock(classOf[MessageSink])
    when(mockMessageSink.needsMoreMessages(any())).thenReturn(true)

    val doNothingMetrics = new KafkaSystemConsumerMetrics()

    val tp = new TopicAndPartition("topic", 42)

    val mockOffsetGetter = mock(classOf[GetOffset])
    // This will be used by the simple consumer below, and this is the response that simple consumer needs
    when(mockOffsetGetter.isValidOffset(any(classOf[DefaultFetchSimpleConsumer]), Matchers.eq(tp), Matchers.eq("0"))).thenReturn(true)
    when(mockOffsetGetter.getResetOffset(any(classOf[DefaultFetchSimpleConsumer]), Matchers.eq(tp))).thenReturn(1492l)

    var callsToCreateSimpleConsumer = 0
    val mockSimpleConsumer = mock(classOf[DefaultFetchSimpleConsumer])

    // Create an answer that first indicates offset out of range on first invocation and on second
    // verifies that the parameters have been updated to what we expect them to be
    val answer = new Answer[FetchResponse]() {
      var invocationCount = 0

      def answer(invocation: InvocationOnMock): FetchResponse = {
        val arguments = invocation.getArguments()(0).asInstanceOf[List[Object]](0).asInstanceOf[(String, Long)]

        if (invocationCount == 0) {
          if (arguments !=(tp, 0)) {
            failString = "First invocation did not have the right arguments: " + arguments
            countdownLatch.countDown()
          }
          val mfr = mock(classOf[FetchResponse])
          when(mfr.hasError).thenReturn(true)
          when(mfr.errorCode("topic", 42)).thenReturn(ErrorMapping.OffsetOutOfRangeCode)

          val messageSet = mock(classOf[MessageSet])
          when(messageSet.iterator).thenReturn(Iterator.empty)
          val response = mock(classOf[FetchResponsePartitionData])
          when(response.error).thenReturn(ErrorMapping.OffsetOutOfRangeCode)
          val responseMap = Map(tp -> response)
          when(mfr.data).thenReturn(responseMap)
          invocationCount += 1
          mfr
        } else {
          if (arguments !=(tp, 1492)) {
            failString = "On second invocation, arguments were not correct: " + arguments
          }
          countdownLatch.countDown()
          Thread.currentThread().interrupt()
          null
        }
      }
    }

    when(mockSimpleConsumer.defaultFetch(any())).thenAnswer(answer)

    // So now we have a fetch response that will fail.  Prime the mockGetOffset to send us to a new offset

    val bp = new BrokerProxy("host", 423, "system", "clientID", doNothingMetrics, mockMessageSink, Int.MaxValue, 1024000, new StreamFetchSizes(256 * 1024), 524288, 1000, mockOffsetGetter) {

      override def createSimpleConsumer() = {
        if (callsToCreateSimpleConsumer > 1) {
          failString = "Tried to create more than one simple consumer"
          countdownLatch.countDown()
        }
        callsToCreateSimpleConsumer += 1
        mockSimpleConsumer
      }
    }

    bp.addTopicPartition(tp, Option("0"))
    bp.start
    countdownLatch.await()
    bp.stop
    if (failString != null) {
      fail(failString)
    }
  }

  /**
   * Test that makes sure that BrokerProxy abdicates all TopicAndPartitions
   * that it owns when a consumer failure occurs.
   */
  @Test def brokerProxyAbdicatesOnConnectionFailure(): Unit = {
    val countdownLatch = new CountDownLatch(1)
    var abdicated: Option[TopicAndPartition] = None
    @volatile var refreshDroppedCount = 0
    val mockMessageSink = new MessageSink {
      override def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      }

      override def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) {
      }

      override def abdicate(tp: TopicAndPartition, nextOffset: Long) {
        abdicated = Some(tp)
        countdownLatch.countDown
      }

      override def refreshDropped() {
        refreshDroppedCount += 1
      }

      override def needsMoreMessages(tp: TopicAndPartition): Boolean = {
        true
      }
    }

    val doNothingMetrics = new KafkaSystemConsumerMetrics()
    val tp = new TopicAndPartition("topic", 42)
    val mockOffsetGetter = mock(classOf[GetOffset])
    val mockSimpleConsumer = mock(classOf[DefaultFetchSimpleConsumer])

    when(mockOffsetGetter.isValidOffset(any(classOf[DefaultFetchSimpleConsumer]), Matchers.eq(tp), Matchers.eq("0"))).thenReturn(true)
    when(mockOffsetGetter.getResetOffset(any(classOf[DefaultFetchSimpleConsumer]), Matchers.eq(tp))).thenReturn(1492l)
    when(mockSimpleConsumer.defaultFetch(any())).thenThrow(new SamzaException("Pretend this is a ClosedChannelException. Can't use ClosedChannelException because it's checked, and Mockito doesn't like that."))

    val bp = new BrokerProxy("host", 567, "system", "clientID", doNothingMetrics, mockMessageSink, Int.MaxValue, 1024000, new StreamFetchSizes(256 * 1024), 524288, 1000, mockOffsetGetter) {
      override def createSimpleConsumer() = {
        mockSimpleConsumer
      }
    }

    val waitForRefresh = () => {
      val currentRefreshDroppedCount = refreshDroppedCount
      while (refreshDroppedCount == currentRefreshDroppedCount) {
        Thread.sleep(100)
      }
    }

    bp.addTopicPartition(tp, Option("0"))
    bp.start
    // BP should refresh on startup.
    waitForRefresh()
    countdownLatch.await()
    // BP should continue refreshing after it's abdicated all TopicAndPartitions.
    waitForRefresh()
    bp.stop
    assertEquals(tp, abdicated.getOrElse(null))
  }

  @Test def brokerProxyAbdicatesHardErrors(): Unit = {
    val doNothingMetrics = new KafkaSystemConsumerMetrics
    val mockMessageSink = new MessageSink {
      override def needsMoreMessages(tp: TopicAndPartition): Boolean = true
      override def abdicate(tp: TopicAndPartition, nextOffset: Long) {}
      override def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) {}
      override def refreshDropped() {throw new OutOfMemoryError("Test - OOME")}
      override def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean): Unit = {}
    }
    val mockOffsetGetter = mock(classOf[GetOffset])
    val mockSimpleConsumer = mock(classOf[DefaultFetchSimpleConsumer])

    val bp = new BrokerProxy("host", 658, "system", "clientID", doNothingMetrics, mockMessageSink, Int.MaxValue, 1024000, new StreamFetchSizes(256 * 1024), 524288, 1000, mockOffsetGetter) {
      override def createSimpleConsumer() = {
        mockSimpleConsumer
      }
    }
    var caughtError = false
    try {
      bp.thread.run
    } catch {
      case e: SamzaException => {
        assertEquals(e.getMessage, "Got out of memory error in broker proxy thread.")
        info("Received OutOfMemoryError in broker proxy.")
        caughtError = true
      }
    }
    assertEquals(true, caughtError)
    val mockMessageSink2 = new MessageSink {
      override def needsMoreMessages(tp: TopicAndPartition): Boolean = true
      override def abdicate(tp: TopicAndPartition, nextOffset: Long): Unit = {}
      override def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long): Unit = {}
      override def refreshDropped(): Unit = {throw new StackOverflowError("Test - SOE")}
      override def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean): Unit = {}
    }
    caughtError = false
    val bp2 = new BrokerProxy("host", 689, "system", "clientID2", doNothingMetrics, mockMessageSink2, Int.MaxValue, 1024000, new StreamFetchSizes(256 * 1024), 524288, 1000, mockOffsetGetter) {
      override def createSimpleConsumer() = {
        mockSimpleConsumer
      }
    }
    try {
      bp2.thread.run
    } catch {
      case e: SamzaException => {
        assertEquals(e.getMessage, "Got stack overflow error in broker proxy thread.")
        info("Received StackOverflowError in broker proxy.")
        caughtError = true
      }
    }
    assertEquals(true, caughtError)
  }

  @Test
	def brokerProxyStopCloseConsumer: Unit = {
    val mockSimpleConsumer = mock(classOf[DefaultFetchSimpleConsumer])
    val bp = new BrokerProxy("host", 0, "system", "clientID", new KafkaSystemConsumerMetrics(), null){
      override def createSimpleConsumer() = {
        mockSimpleConsumer
      }
    }
    bp.start
    bp.stop
    verify(mockSimpleConsumer).close
  }
}
