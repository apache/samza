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

package org.apache.samza.system.chooser

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.util.BlockingEnvelopeMap
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.config.DefaultChooserConfig
import org.apache.samza.system.SystemStream

class TestDefaultChooser {
  val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
  val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), null, null, 2);
  val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream2", new Partition(0)), null, null, 3);
  val envelope4 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), "123", null, 4);
  val envelope5 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), null, null, 5);
  val envelope6 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), "321", null, 6);
  val envelope7 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 7);

  @Test
  def testDefaultChooserWithBatchingPrioritizationAndBootstrapping {
    val mock0 = new MockMessageChooser
    val mock1 = new MockMessageChooser
    val mock2 = new MockMessageChooser
    val chooser = new DefaultChooser(
      mock0,
      Some(2),
      Map(
        envelope1.getSystemStreamPartition().getSystemStream -> Int.MaxValue,
        envelope2.getSystemStreamPartition().getSystemStream -> 1),
      Map(
        Int.MaxValue -> mock1,
        1 -> mock2),
      Map(
        envelope1.getSystemStreamPartition() -> "123",
        envelope5.getSystemStreamPartition() -> "321"))

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.register(envelope3.getSystemStreamPartition, null)
    chooser.register(envelope5.getSystemStreamPartition, null)
    chooser.start
    assertEquals(null, chooser.choose)

    // Load with a non-bootstrap stream, and should still get null.
    chooser.update(envelope3)
    assertEquals(null, chooser.choose)

    // Load with a bootstrap stream, should get that envelope.
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)

    // Should block envelope3 since we have no message from envelope1's bootstrap stream.
    assertEquals(null, chooser.choose)

    // Load envelope2 from non-bootstrap stream with higher priority than envelope3.
    chooser.update(envelope2)

    // Should block envelope2 since we have no message from envelope1's bootstrap stream.
    assertEquals(null, chooser.choose)

    // Test batching by giving chooser envelope1 and envelope5, both from same stream, but envelope1 should be preferred partition.
    chooser.update(envelope5)
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)

    // Now, envelope5 is still loaded, and we've reached our batchSize limit, so loading envelope1 should still let envelope5 through.
    chooser.update(envelope1)
    assertEquals(envelope5, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    // Now we're back to just envelope3, envelope2. Let's catch up envelope1's SSP using envelope4's offset.
    chooser.update(envelope4)
    assertEquals(envelope4, chooser.choose)

    // Should still block envelopes 1 and 2 because the second partition hasn't caught up yet.
    assertEquals(null, chooser.choose)

    // Now catch up the second partition.
    chooser.update(envelope6)
    assertEquals(envelope6, chooser.choose)

    // Cool, now no streams are being bootstrapped. Envelope2 should be prioritized above envelope3, even though envelope3 was added first.
    assertEquals(envelope2, chooser.choose)

    // We should still batch, and prefer envelope2's partition over envelope7, even though they're both from the same stream.
    chooser.update(envelope7)
    chooser.update(envelope2)
    assertEquals(envelope2, chooser.choose)

    // Now envelope2's partition has passed the batchSize, so we should get 7 next.
    chooser.update(envelope2)
    assertEquals(envelope7, chooser.choose)
    assertEquals(envelope2, chooser.choose)

    // Now we should finally get the lowest priority non-bootstrap stream, envelope3.
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testBootstrapConfig {
    import DefaultChooserConfig.Config2DefaultChooser
    val configMap = Map(
      "task.inputs" -> "kafka.foo,kafka.bar-baz",
      "systems.kafka.streams.bar-baz.samza.bootstrap" -> "true")
    val config = new MapConfig(configMap)
    val bootstrapStreams = config.getBootstrapStreams
    assertEquals(1, bootstrapStreams.size)
    assertTrue(bootstrapStreams.contains(new SystemStream("kafka", "bar-baz")))
  }

  @Test
  def testPriorityConfig {
    import DefaultChooserConfig.Config2DefaultChooser
    val configMap = Map(
      "task.inputs" -> "kafka.foo,kafka.bar-baz",
      "systems.kafka.streams.bar-baz.samza.priority" -> "3")
    val config = new MapConfig(configMap)
    val priorityStreams = config.getPriorityStreams
    assertEquals(1, priorityStreams.size)
    assertEquals(3, priorityStreams(new SystemStream("kafka", "bar-baz")))
  }
}

class MockBlockingEnvelopeMap extends BlockingEnvelopeMap {
  def start = Unit
  def stop = Unit
}