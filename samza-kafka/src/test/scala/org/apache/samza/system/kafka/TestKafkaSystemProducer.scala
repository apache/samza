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

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.{RecordTooLargeException, TimeoutException}
import org.apache.kafka.test.MockSerializer
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemProducerException, SystemStream}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept


class TestKafkaSystemProducer {
  val systemStream = new SystemStream("testSystem", "testStream")
  val someMessage = new OutgoingMessageEnvelope(systemStream, "test".getBytes)
  val StreamNameNullOrEmptyErrorMsg = "Stream Name should be specified in the stream configuration file.";

  @Test
  def testKafkaProducer {
    val mockProducer = new MockProducer(true, new MockSerializer, new MockSerializer)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start
    systemProducer.send("test", someMessage)
    assertEquals(1, systemProducer.producer.asInstanceOf[MockProducer[Array[Byte], Array[Byte]]].history().size())
    systemProducer.stop
  }

  @Test
  def testKafkaProducerUsingMockKafkaProducer {
    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", someMessage)
    assertEquals(1, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerBufferedSend {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producerMetrics = new KafkaSystemProducerMetrics
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = producerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    assertEquals(1, mockProducer.getMsgsSent)

    val sendThread: Thread = mockProducer.startDelayedSendThread(2000)
    sendThread.join()

    assertEquals(3, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerFlushSuccessful {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    assertEquals(1, mockProducer.getMsgsSent)
    mockProducer.startDelayedSendThread(2000)
    systemProducer.flush("test")
    assertEquals(3, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerFlushWithException {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics())
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    mockProducer.setErrorNext(true, new RecordTooLargeException())
    systemProducer.send("test", msg3)
    systemProducer.send("test", msg4)

    assertEquals(1, mockProducer.getMsgsSent)

    mockProducer.startDelayedSendThread(2000)
    val thrown = intercept[SystemProducerException] {
      systemProducer.flush("test")
    }
    assertTrue(thrown.isInstanceOf[SystemProducerException])
    assertEquals(3, mockProducer.getMsgsSent) // msg1, msg2 and msg4 will be sent
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerWithRetriableException {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producerMetrics = new KafkaSystemProducerMetrics()
    val producer = new KafkaSystemProducer(systemName =  "test",
      getProducer = () => mockProducer,
      metrics = producerMetrics)

    producer.register("test")
    producer.start()
    producer.send("test", msg1)
    producer.send("test", msg2)
    producer.send("test", msg3)
    producer.flush("test")

    mockProducer.setErrorNext(true, new TimeoutException())

    producer.send("test", msg4)
    val thrown = intercept[SystemProducerException] {
      producer.flush("test")
    }
    assertTrue(thrown.isInstanceOf[SystemProducerException])
    assertTrue(thrown.getCause.getCause.isInstanceOf[TimeoutException])
    assertEquals(3, mockProducer.getMsgsSent)
    producer.stop()
  }

  /**
    * If there's an exception, we should:
    * 1. Close the producer (from the one-and-only kafka send thread) to prevent subsequent sends from going out of order.
    * 2. Nullify the producer to cause it to be recreated
    * 3. Throw the exception from systemProducer.flush() to prevent a checkpoint
    *
    * Assumptions:
    * 1. SystemProducer.flush() can happen concurrently with SystemProducer.send() for a particular TaskInstance (task.async.commit)
    * 2. SystemProducer.flush() cannot happen concurrently with itself for a particular task instance
    * 3. Any exception thrown from SystemProducer.flush() will prevent the checkpointing by failing the container
    * 4. The exception should not fail the container if SystemProducerException is configured to be ignored
    *
    * Conclusions:
    * It is only safe to handle the async exceptions from flush to prevent race conditions between exception handling
    * from send/flush when async.commit is enabled.
    *
    * Inaccuracies:
    * A real kafka producer succeeds or fails all the messages in a batch. In other words, the messages of a batch all
    * fail or they all succeed together. This test, however, fails individual callbacks in order to test boundary
    * conditions where the batches align perfectly around the failed send().
    */
  @Test
  def testKafkaProducerWithNonRetriableExceptions {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
                                           getProducer = () => mockProducer,
                                           metrics = producerMetrics)
    producer.register("test")
    producer.start()

    producer.send("test", msg1)
    producer.send("test", msg2)
    mockProducer.setErrorNext(true, new RecordTooLargeException())
    producer.send("test", msg3) // Callback exception
    producer.send("test", msg4) // Should still work
    val thrown = intercept[SystemProducerException] {
       producer.flush("test") // Should throw the callback exception
    }
    assertTrue(thrown.isInstanceOf[SystemProducerException])
    assertTrue(thrown.getCause.getCause.isInstanceOf[RecordTooLargeException])

    producer.flush("test") // Should not rethrow the exception
    assertEquals(3, mockProducer.getMsgsSent)
    producer.stop()
  }

  /**
    * Recapping from [[testKafkaProducerWithNonRetriableExceptions]]:
    *
    * If there's an exception, we should:
    * 1. Close the producer (from the one-and-only kafka send thread) to prevent subsequent sends from going out of order.
    * 2. Nullify the producer to cause it to be recreated
    * 3. Throw the exception from systemProducer.flush() to prevent a checkpoint
    *
    * This test focuses on point 3. Particularly it ensures that the failures are isolated to the affected source.
    */
  @Test
  def testKafkaProducerWithNonRetriableExceptionsMultipleSources {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
      getProducer = () => mockProducer,
      metrics = producerMetrics)
    producer.register("test1")
    producer.register("test2")

    producer.start()

    // Initial sends
    producer.send("test1", msg1)
    producer.send("test2", msg2)

    // Inject error for next send
    mockProducer.setErrorNext(true, new RecordTooLargeException())
    producer.send("test1", msg3) // Callback exception

    // Subsequent sends
    producer.send("test1", msg4) // Should still work
    producer.send("test2", msg4)

    // Flushes
    producer.flush("test2") // Should not be affected by the exception for source 1
    val thrown = intercept[SystemProducerException] {
      producer.flush("test1") // Should throw the callback exception
    }
    assertTrue(thrown.isInstanceOf[SystemProducerException])
    assertTrue(thrown.getCause.getCause.isInstanceOf[RecordTooLargeException])

    producer.flush("test1") // Should not rethrow
    assertEquals(4, mockProducer.getMsgsSent)
    producer.stop()
  }

  @Test
  def testKafkaProducerFlushMsgsWhenStop {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(new SystemStream("test2", "test"), "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.register("test2")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    systemProducer.send("test2", msg4)
    assertEquals(1, mockProducer.getMsgsSent)
    mockProducer.startDelayedSendThread(2000)
    assertEquals(1, mockProducer.getMsgsSent)
    systemProducer.stop()
    assertEquals(4, mockProducer.getMsgsSent)
  }

  @Test
  def testSystemStreamNameNullOrEmpty {
    val omeStreamNameNull = new OutgoingMessageEnvelope(new SystemStream("test", null), "a".getBytes)
    val omeStreamNameEmpty = new OutgoingMessageEnvelope(new SystemStream("test", ""), "a".getBytes)
    val mockProducer = new MockKafkaProducer(1, "testMock", 1)
    val producer = new KafkaSystemProducer(systemName = "test", getProducer = () => mockProducer,
                                           metrics = new KafkaSystemProducerMetrics)

    val thrownNull = intercept[IllegalArgumentException] {
      producer.register("test1")
      producer.start()
      producer.send("testSrc1", omeStreamNameNull)
      assertEquals(0, mockProducer.getMsgsSent)
    }
    val thrownEmpty = intercept[IllegalArgumentException] {
      producer.register("test2")
      producer.start()
      producer.send("testSrc2", omeStreamNameEmpty)
      assertEquals(0, mockProducer.getMsgsSent)
    }
    assertTrue(thrownNull.getMessage() == StreamNameNullOrEmptyErrorMsg)
    assertTrue(thrownEmpty.getMessage() == StreamNameNullOrEmptyErrorMsg)
  }
}
