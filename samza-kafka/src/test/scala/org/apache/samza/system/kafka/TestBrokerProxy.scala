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

import org.junit._
import org.junit.Assert._
import org.apache.samza.config.MapConfig
import org.mockito.Mockito
import org.apache.samza.metrics._
import scala.collection.JavaConversions._
import kafka.consumer.SimpleConsumer
import org.mockito.Mockito._
import org.mockito.Matchers._
import kafka.api._
import kafka.message.{ Message, MessageSet, MessageAndOffset }
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetsResponse
import java.nio.ByteBuffer
import org.apache.samza.SamzaException
import kafka.message.ByteBufferMessageSet

class TestBrokerProxy {
  val tp2 = new TopicAndPartition("Redbird", 2013)

  def getMockBrokerProxy() = {
    val sink = new MessageSink {
      val receivedMessages = new scala.collection.mutable.ListBuffer[(TopicAndPartition, MessageAndOffset, Boolean)]()
      def abdicate(tp: TopicAndPartition, lastOffset: Long) {}

      def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) { receivedMessages.add((tp, msg, msg.offset.equals(highWatermark))) }

      def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      }

      // Never need messages for tp2.
      def needsMoreMessages(tp: TopicAndPartition): Boolean = !tp.equals(tp2)
    }

    val config = new MapConfig(Map[String, String]("job.name" -> "Jobby McJob",
      "systems.daSystem.Redbird.consumer.auto.offset.reset" -> "largest"))
    val metricsRegistry = {
      val registry = Mockito.mock(classOf[MetricsRegistry])
      val gauge = Mockito.mock(classOf[Gauge[Long]])
      when(gauge.getValue()).thenReturn(0l)
      when(registry.newGauge[Long](anyString(), anyString(), anyLong())).thenReturn(gauge)

      val counter = Mockito.mock(classOf[Counter])
      when(registry.newCounter(anyString(), anyString())).thenReturn(counter)
      registry
    }

    val tp = new TopicAndPartition("Redbird", 2012)
    val tpMetrics = new TopicAndPartitionMetrics(metricsRegistry)

    tpMetrics.addNewTopicAndPartition(tp)

    val bp = new BrokerProxy(
      "host",
      2222,
      "daSystem",
      "daClientId",
      metricsRegistry,
      tpMetrics,
      offsetGetter = new GetOffset("fail", Map("Redbird" -> "largest"))) {

      val messageSink: MessageSink = sink

      override val sleepMSWhileNoTopicPartitions = 100 // Speed up for test
      var alreadyCreatedConsumer = false
      // Scala traits and Mockito mocks don't mix, unfortunately.
      override def createSimpleConsumer() = {
        if (alreadyCreatedConsumer) {
          System.err.println("Should only be creating one consumer in this test!")
          throw new InterruptedException("Should only be creating one consumer in this test!")
        }
        alreadyCreatedConsumer = true

        new SimpleConsumer("a", 1, 2, 3, "b") with DefaultFetch {
          val fetchSize: Int = 42

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

          override def getOffsetsBefore(request: OffsetRequest): OffsetResponse = sc.getOffsetsBefore(request)

          override def commitOffsets(request: OffsetCommitRequest): OffsetCommitResponse = sc.commitOffsets(request)

          override def fetchOffsets(request: OffsetFetchRequest): OffsetFetchResponse = sc.fetchOffsets(request)

          override def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = sc.earliestOrLatestOffset(topicAndPartition, earliestOrLatest, consumerId)
        }
      }

    }

    (bp, tp, sink)
  }

  @Test def brokerProxyRetrievesMessagesCorrectly() = {
    val (bp, tp, sink) = getMockBrokerProxy()

    bp.start
    bp.addTopicPartition(tp, "0")
    // Add tp2, which should never receive messages since sink disables it.
    bp.addTopicPartition(tp2, "0")
    Thread.sleep(1000)
    assertEquals(2, sink.receivedMessages.size)
    assertEquals(42, sink.receivedMessages.get(0)._2.offset)
    assertEquals(84, sink.receivedMessages.get(1)._2.offset)
  }

  @Test def brokerProxyThrowsExceptionOnDuplicateTopicPartitions() = {
    val (bp, tp, _) = getMockBrokerProxy()
    bp.start
    bp.addTopicPartition(tp, "0")

    try {
      bp.addTopicPartition(tp, "1")
      fail("Should have thrown an exception")
    } catch {
      case se: SamzaException => assertEquals(se.getMessage, "Already consuming TopicPartition [Redbird,2012]")
    }
  }

}
