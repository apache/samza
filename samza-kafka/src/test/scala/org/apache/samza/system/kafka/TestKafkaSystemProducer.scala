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
import org.apache.kafka.common.errors.{RecordTooLargeException, SerializationException, TimeoutException}
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
    mockProducer.setErrorNext(true, true, new RecordTooLargeException())
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

    mockProducer.setErrorNext(true, true, new TimeoutException())

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
    * 2. Record the original exception
    * 3. Throw the exception every time a KafkaSystemProducer method is invoked until the container fails.
    *
    * Assumptions:
    * 1. SystemProducer.flush() can happen concurrently with SystemProducer.send() for a particular TaskInstance (task.async.commit)
    * 2. SystemProducer.flush() cannot happen concurrently with itself for a particular task instance
    * 3. Any exception thrown from SystemProducer.flush() will prevent the checkpointing and fail the container
    * 4. A single KafkaProducer is shared by all the tasks so any failure from one task can affect the others.
    *
    * Conclusions:
    * It is only safe to handle the async exceptions from by closing the producer and failing the container.
    * This prevents race conditons with setting/clearing exceptions and recreating the producer that could cause data
    * loss by checkpointing a failed offset.
    *
    * Inaccuracies:
    * A real kafka producer succeeds or fails all the messages in a batch. In other words, the messages of a batch all
    * fail or they all succeed together. This test, however, fails individual callbacks in order to test boundary
    * conditions where the batches align perfectly around the failed send().
    */
  @Test
  def testKafkaProducerWithFatalExceptions {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
                                           getProducer = () => {
                                             mockProducer.open() // A new producer is never closed
                                             mockProducer
                                           },
                                           metrics = producerMetrics)
    producer.register("test")
    producer.start()

    producer.send("test", msg1)
    producer.send("test", msg2)
    mockProducer.setErrorNext(true, true, new RecordTooLargeException())
    producer.send("test", msg3) // Callback exception
    assertTrue(mockProducer.isClosed)
    assertEquals("Should NOT have created a new producer", 1, mockProducer.getOpenCount)

    val senderException = intercept[SystemProducerException] {
      producer.send("test", msg4) // Should fail because the producer is closed.
    }
    assertTrue(senderException.getCause.getCause.isInstanceOf[RecordTooLargeException])

    val callbackException = intercept[SystemProducerException] {
       producer.flush("test") // Should throw the callback exception
    }
    assertTrue(callbackException.getCause.getCause.isInstanceOf[RecordTooLargeException])

    val postFlushException = intercept[SystemProducerException] {
      producer.send("test", msg5) // Should not be able to send again after flush
    }
    assertTrue(postFlushException.getCause.getCause.isInstanceOf[RecordTooLargeException])

    val callbackException2 = intercept[SystemProducerException] {
      producer.flush("test") // Should rethrow the exception
    }
    assertTrue(callbackException2.getCause.getCause.isInstanceOf[RecordTooLargeException])
    assertEquals(2, mockProducer.getMsgsSent) // only the messages before the error get sent
    producer.stop()
  }

  /**
    * Recapping from [[testKafkaProducerWithFatalExceptions]]:
    *
    * If there's an exception, we should:
    * 1. Close the producer (from the one-and-only kafka send thread) to prevent subsequent sends from going out of order.
    * 2. Record the original exception
    * 3. Throw the exception every time a KafkaSystemProducer method is invoked until the container fails.
    *
    * This test focuses on point 3. Particularly it ensures that the failures are handled properly across multiple sources
    * which share the same producer.
    */
  @Test
  def testKafkaProducerWithFatalExceptionsMultipleSources {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val msg6 = new OutgoingMessageEnvelope(systemStream, "f".getBytes)
    val msg7 = new OutgoingMessageEnvelope(systemStream, "g".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
      getProducer = () => {
        mockProducer.open() // A new producer is never closed
        mockProducer
      },
      metrics = producerMetrics)
    producer.register("test1")
    producer.register("test2")

    producer.start()

    // Initial sends
    producer.send("test1", msg1)
    producer.send("test2", msg2)

    // Inject error for next send
    mockProducer.setErrorNext(true, true, new RecordTooLargeException())
    producer.send("test1", msg3) // Callback exception
    assertTrue(mockProducer.isClosed)
    assertEquals("Should NOT have created a new producer", 1, mockProducer.getOpenCount)

    // Subsequent sends
    val senderException = intercept[SystemProducerException] {
      producer.send("test1", msg4) // Should fail because the producer is closed.
    }
    assertTrue(senderException.getCause.getCause.isInstanceOf[RecordTooLargeException])

    val callbackException = intercept[SystemProducerException] {
      producer.send("test2", msg4) // First send from separate source gets a producer closed exception
    }
    assertTrue(callbackException.getCause.getCause.isInstanceOf[RecordTooLargeException])

    val callbackException2 = intercept[SystemProducerException] {
      producer.send("test2", msg5) // Second send should still get the error
    }
    assertTrue(callbackException2.getCause.getCause.isInstanceOf[RecordTooLargeException])

    // Flushes
    val callbackException3 = intercept[SystemProducerException] {
      producer.flush("test2") // Should rethrow the closed exception in flush
    }
    assertTrue(callbackException3.isInstanceOf[SystemProducerException])
    assertTrue(callbackException3.getCause.getCause.isInstanceOf[RecordTooLargeException])
    intercept[SystemProducerException] {
      producer.send("test2", msg6) // Should still not be able to send after flush
    }

    val thrown3 = intercept[SystemProducerException] {
      producer.flush("test1") // Should throw the callback exception
    }
    assertTrue(thrown3.isInstanceOf[SystemProducerException])
    assertTrue(thrown3.getCause.getCause.isInstanceOf[RecordTooLargeException])

    intercept[SystemProducerException] {
      producer.send("test1", msg7) // Should still not be able to send after flush
    }

    intercept[SystemProducerException] {
      producer.flush("test1") // Should throw the callback exception
    }
    assertEquals(2, mockProducer.getMsgsSent)
    producer.stop()
  }

  @Test
  def testKafkaProducerWithNonFatalExceptionsMultipleSources {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName = "test",
      getProducer = () => {
        mockProducer.open() // A new producer is never closed
        mockProducer
      },
      metrics = producerMetrics)
    producer.register("test1")
    producer.register("test2")
    producer.start()

    producer.send("test1", msg1)
    producer.send("test2", msg2)
    mockProducer.setErrorNext(true, false, new SerializationException())
    val sendException = intercept[SystemProducerException] {
      producer.send("test1", msg3) // User-thread exception
    }
    assertTrue(sendException.getCause.isInstanceOf[SerializationException])
    assertFalse(mockProducer.isClosed)
    assertEquals("Should NOT have created a new producer", 1, mockProducer.getOpenCount)

    producer.send("test1", msg3) // Should be able to resend msg3
    producer.send("test2", msg4) // Second source should not be affected

    producer.flush("test1") // Flush should be unaffected

    producer.send("test1", msg5) // Should be able to send again after flush

    assertEquals(5, mockProducer.getMsgsSent) // only the messages before the error get sent
    producer.stop()
  }

  /**
    * If there's an exception and the user configured task.drop.producer.errors, we should:
    * 1. Close the producer (from the one-and-only kafka send thread) to prevent subsequent sends from going out of order.
    * 2. Recreate the producer.
    * 3. Ignore any messages that were dropped (user knows they're signing up for this if they enable the option)
    *
    * Assumptions:
    * 1. SystemProducer.flush() can happen concurrently with SystemProducer.send() for a particular TaskInstance (task.async.commit)
    * 2. SystemProducer.flush() cannot happen concurrently with itself for a particular task instance
    * 3. Any exception thrown from SystemProducer.flush() will prevent the checkpointing and fail the container
    * 4. A single KafkaProducer is shared by all the tasks so any failure from one task can affect the others.
    *
    * Conclusions:
    * If the user is ok with dropping messages for the sake of availability, we will swallow all exceptions and
    * recreate the producer to recover. There are no guarantees how many messages are lost, but the send-failed metric
    * should be accurate, so users should alert on that.
    *
    * Inaccuracies:
    * A real kafka producer succeeds or fails all the messages in a batch. In other words, the messages of a batch all
    * fail or they all succeed together. This test, however, fails individual callbacks in order to test boundary
    * conditions where the batches align perfectly around the failed send().
    */
  @Test
  def testKafkaProducerWithFatalExceptionsDroppingExceptions {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
      getProducer = () => {
        mockProducer.open() // A new producer is never closed
        mockProducer
      },
      metrics = producerMetrics,
      dropProducerExceptions = true)  // Here's where we enable exception dropping.
    producer.register("test")
    producer.start()

    producer.send("test", msg1)
    producer.send("test", msg2)
    mockProducer.setErrorNext(true, true, new RecordTooLargeException())
    producer.send("test", msg3) // Callback exception
    assertFalse(mockProducer.isClosed)
    assertEquals("Should have created a new producer", 2, mockProducer.getOpenCount)

    producer.send("test", msg4) // Should succeed because the producer recovered.
    producer.flush("test") // Should not throw

    producer.send("test", msg5) // Should be able to send again after flush
    producer.flush("test")

    assertEquals(4, mockProducer.getMsgsSent) // every message except the one with the error should get sent
    producer.stop()
  }

  /**
    * Recapping from [[testKafkaProducerWithFatalExceptionsDroppingExceptions]]:
    *
    * If there's an exception, we should:
    * 1. Close the producer (from the one-and-only kafka send thread) to prevent subsequent sends from going out of order.
    * 2. Recreate the producer.
    * 3. Ignore any messages that were dropped (user knows they're signing up for this if they enable the option)
    *
    * This test ensures that the failures are handled properly across multiple sources
    * which share the same producer.
    */
  @Test
  def testKafkaProducerWithFatalExceptionsMultipleSourcesDroppingExceptions {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val msg6 = new OutgoingMessageEnvelope(systemStream, "f".getBytes)
    val msg7 = new OutgoingMessageEnvelope(systemStream, "g".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
      getProducer = () => {
        mockProducer.open() // A new producer is never closed
        mockProducer
      },
      metrics = producerMetrics,
      dropProducerExceptions = true)  // Here's where we enable exception dropping.
    producer.register("test1")
    producer.register("test2")

    producer.start()

    // Initial sends
    producer.send("test1", msg1)
    producer.send("test2", msg2)

    // Inject error for next send
    mockProducer.setErrorNext(true, true, new RecordTooLargeException())
    producer.send("test1", msg3) // Callback exception
    assertFalse(mockProducer.isClosed)
    assertEquals("Should have created a new producer", 2, mockProducer.getOpenCount)

    // Subsequent sends
    producer.send("test1", msg4) // Should succeed because the producer recovered.
    producer.send("test2", msg5) // Second source should also not have any error.

    // Flushes
    producer.flush("test2") // Should not throw for test2
    producer.send("test2", msg6) // Should still work after flush

    producer.flush("test1") // Should not throw for test1 either
    producer.send("test1", msg7)

    assertEquals(6, mockProducer.getMsgsSent) // every message except the one with the error should get sent
    producer.stop()
  }

  @Test
  def testKafkaProducerWithNonFatalExceptionsMultipleSourcesDroppingExceptions {
    val msg1 = new OutgoingMessageEnvelope(systemStream, "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(systemStream, "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(systemStream, "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(systemStream, "d".getBytes)
    val msg5 = new OutgoingMessageEnvelope(systemStream, "e".getBytes)
    val producerMetrics = new KafkaSystemProducerMetrics()

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName = "test",
      getProducer = () => {
        mockProducer.open() // A new producer is never closed
        mockProducer
      },
      metrics = producerMetrics,
      dropProducerExceptions = true)  // Here's where we enable exception dropping.
    producer.register("test1")
    producer.register("test2")
    producer.start()

    producer.send("test1", msg1)
    producer.send("test2", msg2)
    mockProducer.setErrorNext(true, false, new SerializationException())
    val sendException = intercept[SystemProducerException] {
      producer.send("test1", msg3) // User-thread exception
    }
    assertTrue(sendException.getCause.isInstanceOf[SerializationException])
    assertFalse(mockProducer.isClosed)
    assertEquals("Should NOT have created a new producer", 1, mockProducer.getOpenCount)

    producer.send("test1", msg3) // Should be able to resend msg3
    producer.send("test2", msg4) // Second source should not be affected

    producer.flush("test1") // Flush should be unaffected

    producer.send("test1", msg5) // Should be able to send again after flush

    assertEquals(5, mockProducer.getMsgsSent) // only the messages before the error get sent
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
