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

package org.apache.samza.system

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.Partition
import org.apache.samza.serializers._
import org.apache.samza.system.chooser.MessageChooser
import org.apache.samza.system.chooser.DefaultChooser
import org.apache.samza.system.chooser.MockMessageChooser
import org.apache.samza.util.BlockingEnvelopeMap

import scala.collection.JavaConversions._

class TestSystemConsumers {
  def testPollIntervalMs {
    val numEnvelopes = 1000
    val system = "test-system"
    val systemStreamPartition0 = new SystemStreamPartition(system, "some-stream", new Partition(0))
    val systemStreamPartition1 = new SystemStreamPartition(system, "some-stream", new Partition(1))
    val envelope = new IncomingMessageEnvelope(systemStreamPartition0, "1", "k", "v")
    val consumer = new CustomPollResponseSystemConsumer(envelope)
    var now = 0L
    val consumers = new SystemConsumers(new MockMessageChooser, Map(system -> consumer), clock = () => now)

    consumers.register(systemStreamPartition0, "0")
    consumers.register(systemStreamPartition1, "1234")
    consumers.start

    // Tell the consumer to respond with 1000 messages for SSP0, and no
    // messages for SSP1.
    consumer.setResponseSizes(numEnvelopes)

    // Choose to trigger a refresh with data.
    assertNull(consumers.choose)
    // 2: First on start, second on choose.
    assertEquals(2, consumer.polls)
    assertEquals(2, consumer.lastPoll.size)
    assertTrue(consumer.lastPoll.contains(systemStreamPartition0))
    assertTrue(consumer.lastPoll.contains(systemStreamPartition1))
    assertEquals(envelope, consumers.choose)
    assertEquals(envelope, consumers.choose)
    // We aren't polling because we're getting non-null envelopes.
    assertEquals(2, consumer.polls)

    // Advance the clock to trigger a new poll even though there are still
    // messages.
    now = SystemConsumers.DEFAULT_POLL_INTERVAL_MS

    assertEquals(envelope, consumers.choose)

    // We polled even though there are still 997 messages in the unprocessed
    // message buffer.
    assertEquals(3, consumer.polls)
    assertEquals(1, consumer.lastPoll.size)

    // Only SSP1 was polled because we still have messages for SSP2.
    assertTrue(consumer.lastPoll.contains(systemStreamPartition1))

    // Now drain all messages for SSP0. There should be exactly 997 messages,
    // since we have chosen 3 already, and we started with 1000.
    (0 until (numEnvelopes - 3)).foreach { i =>
      assertEquals(envelope, consumers.choose)
    }

    // Nothing left. Should trigger a poll here.
    assertNull(consumers.choose)
    assertEquals(4, consumer.polls)
    assertEquals(2, consumer.lastPoll.size)

    // Now we ask for messages from both again.
    assertTrue(consumer.lastPoll.contains(systemStreamPartition0))
    assertTrue(consumer.lastPoll.contains(systemStreamPartition1))
  }

  def testBasicSystemConsumersFunctionality {
    val system = "test-system"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    val envelope = new IncomingMessageEnvelope(systemStreamPartition, "1", "k", "v")
    val consumer = new CustomPollResponseSystemConsumer(envelope)
    var now = 0
    val consumers = new SystemConsumers(new MockMessageChooser, Map(system -> consumer), clock = () => now)

    consumers.register(systemStreamPartition, "0")
    consumers.start

    // Start should trigger a poll to the consumer.
    assertEquals(1, consumer.polls)

    // Tell the consumer to start returning messages when polled.
    consumer.setResponseSizes(1)

    // Choose to trigger a refresh with data.
    assertNull(consumers.choose)

    // Choose should have triggered a second poll, since no messages are available.
    assertEquals(2, consumer.polls)

    // Choose a few times. This time there is no data.
    assertEquals(envelope, consumers.choose)
    assertNull(consumers.choose)
    assertNull(consumers.choose)

    // Return more than one message this time.
    consumer.setResponseSizes(2)

    // Choose to trigger a refresh with data.
    assertNull(consumers.choose)

    // Increase clock interval.
    now = SystemConsumers.DEFAULT_POLL_INTERVAL_MS

    // We get two messages now.
    assertEquals(envelope, consumers.choose)
    // Should not poll even though clock interval increases past interval threshold.
    assertEquals(2, consumer.polls)
    assertEquals(envelope, consumers.choose)
    assertNull(consumers.choose)
  }

  @Test
  def testSystemConumersShouldRegisterStartAndStopChooser {
    val system = "test-system"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    var consumerStarted = 0
    var consumerStopped = 0
    var consumerRegistered = Map[SystemStreamPartition, String]()
    var chooserStarted = 0
    var chooserStopped = 0
    var chooserRegistered = Map[SystemStreamPartition, String]()

    val consumer = Map(system -> new SystemConsumer {
      def start = consumerStarted += 1
      def stop = consumerStopped += 1
      def register(systemStreamPartition: SystemStreamPartition, offset: String) = consumerRegistered += systemStreamPartition -> offset
      def poll(systemStreamPartitions: java.util.Set[SystemStreamPartition], timeout: Long) = Map[SystemStreamPartition, java.util.List[IncomingMessageEnvelope]]()
    })

    val consumers = new SystemConsumers(new MessageChooser {
      def update(envelope: IncomingMessageEnvelope) = Unit
      def choose = null
      def start = chooserStarted += 1
      def stop = chooserStopped += 1
      def register(systemStreamPartition: SystemStreamPartition, offset: String) = chooserRegistered += systemStreamPartition -> offset
    }, consumer, null)

    consumers.register(systemStreamPartition, "0")
    consumers.start
    consumers.stop

    assertEquals(1, chooserStarted)
    assertEquals(1, chooserStopped)
    assertEquals(1, chooserRegistered.size)
    assertEquals("0", chooserRegistered(systemStreamPartition))

    assertEquals(1, consumerStarted)
    assertEquals(1, consumerStopped)
    assertEquals(1, consumerRegistered.size)
    assertEquals("0", consumerRegistered(systemStreamPartition))
  }

  @Test
  def testThrowSystemConsumersExceptionWhenTheSystemDoesNotHaveConsumer() {
    val system = "test-system"
    val system2 = "test-system2"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    val systemStreamPartition2 = new SystemStreamPartition(system2, "some-stream", new Partition(1))
    var started = 0
    var stopped = 0
    var registered = Map[SystemStreamPartition, String]()

    val consumer = Map(system -> new SystemConsumer {
      def start {}
      def stop {}
      def register(systemStreamPartition: SystemStreamPartition, offset: String) {}
      def poll(systemStreamPartitions: java.util.Set[SystemStreamPartition], timeout: Long) = Map[SystemStreamPartition, java.util.List[IncomingMessageEnvelope]]()
    })
    val consumers = new SystemConsumers(new MessageChooser {
      def update(envelope: IncomingMessageEnvelope) = Unit
      def choose = null
      def start = started += 1
      def stop = stopped += 1
      def register(systemStreamPartition: SystemStreamPartition, offset: String) = registered += systemStreamPartition -> offset
    }, consumer, null)

    // it should throw a SystemConsumersException because system2 does not have a consumer
    var caughtRightException = false
    try {
      consumers.register(systemStreamPartition2, "0")
    } catch {
      case e: SystemConsumersException => caughtRightException = true
      case _: Throwable => caughtRightException = false
    }
    assertTrue("suppose to throw SystemConsumersException, but apparently it did not", caughtRightException)
  }

  @Test
  def testDroppingMsgOrThrowExceptionWhenSerdeFails() {
    val system = "test-system"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    val msgChooser = new DefaultChooser
    val consumer = Map(system -> new SerializingConsumer)
    val systemMessageSerdes = Map(system -> (new StringSerde("UTF-8")).asInstanceOf[Serde[Object]]);
    val serdeManager = new SerdeManager(systemMessageSerdes = systemMessageSerdes)

    // throw exceptions when the deserialization has error
    val consumers = new SystemConsumers(msgChooser, consumer, serdeManager, dropDeserializationError = false)
    consumers.register(systemStreamPartition, "0")
    consumer(system).putBytesMessage
    consumer(system).putStringMessage
    consumers.start

    var caughtRightException = false
    try {
      consumers.choose
    } catch {
      case e: SystemConsumersException => caughtRightException = true
      case _: Throwable => caughtRightException = false
    }
    assertTrue("suppose to throw SystemConsumersException", caughtRightException);
    consumers.stop

    // it should not throw exceptions when deserializaion fails if dropDeserializationError is set to true
    val consumers2 = new SystemConsumers(msgChooser, consumer, serdeManager, dropDeserializationError = true)
    consumers2.register(systemStreamPartition, "0")
    consumer(system).putBytesMessage
    consumer(system).putStringMessage
    consumer(system).putBytesMessage
    consumers2.start

    var notThrowException = true;
    try {
      consumers2.choose
    } catch {
      case e: Throwable => notThrowException = false
    }
    assertTrue("it should not throw any exception", notThrowException)

    var msgEnvelope = Some(consumers2.choose)
    assertTrue("Consumer did not succeed in receiving the second message after Serde exception in choose", msgEnvelope.get != null)
    consumers2.stop

    // ensure that the system consumer will continue after poll() method ignored a Serde exception
    consumer(system).putStringMessage
    consumer(system).putBytesMessage

    notThrowException = true;
    try {
      consumers2.start
    } catch {
      case e: Throwable => notThrowException = false
    }
    assertTrue("SystemConsumer start should not throw any Serde exception", notThrowException)

    msgEnvelope = null
    msgEnvelope = Some(consumers2.choose)
    assertTrue("Consumer did not succeed in receiving the second message after Serde exception in poll", msgEnvelope.get != null)
    consumers2.stop

  }

  /**
   * A simple MockSystemConsumer that keeps track of what was polled, and lets
   * you define how many envelopes to return in the poll response. You can
   * supply the envelope to use for poll responses through the constructor.
   */
  private class CustomPollResponseSystemConsumer(envelope: IncomingMessageEnvelope) extends SystemConsumer {
    var polls = 0
    var pollResponse = Map[SystemStreamPartition, java.util.List[IncomingMessageEnvelope]]()
    var lastPoll: java.util.Set[SystemStreamPartition] = null
    def start {}
    def stop {}
    def register(systemStreamPartition: SystemStreamPartition, offset: String) {}
    def poll(systemStreamPartitions: java.util.Set[SystemStreamPartition], timeout: Long) = {
      polls += 1
      lastPoll = systemStreamPartitions
      pollResponse
    }
    def setResponseSizes(numEnvelopes: Int) {
      val q = new java.util.ArrayList[IncomingMessageEnvelope]()
      (0 until numEnvelopes).foreach { i => q.add(envelope) }
      pollResponse = Map(envelope.getSystemStreamPartition -> q)
      pollResponse = Map[SystemStreamPartition, java.util.List[IncomingMessageEnvelope]]()
    }
  }

  /**
   * A simple consumer that provides two extra methods: one is to put bytes
   * format message and the other to put string format message.
   */
  private class SerializingConsumer extends BlockingEnvelopeMap {
    val systemStreamPartition = new SystemStreamPartition("test-system", "some-stream", new Partition(1))
    def putBytesMessage {
      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, "0", "0", "test".getBytes()))
    }
    def putStringMessage {
      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, "0", "1", "test"))
    }
    def start {}
    def stop {}
    def register { super.register(systemStreamPartition, "0") }
  }
}
