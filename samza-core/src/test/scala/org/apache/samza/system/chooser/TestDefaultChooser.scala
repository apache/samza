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

import org.apache.samza.Partition
import org.apache.samza.config.{DefaultChooserConfig, MapConfig}
import org.apache.samza.container.MockSystemAdmin
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.apache.samza.util.BlockingEnvelopeMap
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class TestDefaultChooser {
  val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), "120", null, 1);
  val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), "121", null, 2);
  val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream2", new Partition(0)), "122", null, 3);
  val envelope4 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), "123", null, 4);
  val envelope5 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), "320", null, 5);
  val envelope6 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), "321", null, 6);
  val envelope7 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), "653", null, 7);
  val envelope8 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream3", new Partition(0)), "654", null, 8);

  @Test
  def testDefaultChooserWithBatchingPrioritizationAndBootstrapping {
    val mock0 = new MockMessageChooser
    val mock1 = new MockMessageChooser
    val mock2 = new MockMessageChooser
    // Create metadata for two envelopes (1 and 5) that are part of the same
    // stream, but have different partitions and offsets.
    val env1Metadata = new SystemStreamPartitionMetadata(null, "123", null)
    val env5Metadata = new SystemStreamPartitionMetadata(null, "321", null)
    val env8Metadata = new SystemStreamPartitionMetadata("0", "456", "654")
    val streamMetadata = new SystemStreamMetadata("stream", Map(
      envelope1.getSystemStreamPartition().getPartition() -> env1Metadata,
      envelope5.getSystemStreamPartition().getPartition() -> env5Metadata).asJava)
    val stream3Metadata = new SystemStreamMetadata("stream3", Map(
      envelope8.getSystemStreamPartition().getPartition() -> env8Metadata).asJava)
    val systemAdmin: SystemAdmin = new MockSystemAdmin()
    val chooser = new DefaultChooser(
      mock0,
      Some(2),
      Map(
        envelope1.getSystemStreamPartition().getSystemStream -> Int.MaxValue,
        envelope8.getSystemStreamPartition().getSystemStream -> Int.MaxValue,
        envelope2.getSystemStreamPartition().getSystemStream -> 1),
      Map(
        Int.MaxValue -> mock1,
        1 -> mock2),
      Map(
        envelope1.getSystemStreamPartition.getSystemStream -> streamMetadata,
        envelope8.getSystemStreamPartition.getSystemStream -> stream3Metadata),
      new MetricsRegistryMap(),
      new SystemAdmins(Map("kafka" -> systemAdmin).asJava))

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.register(envelope3.getSystemStreamPartition, null)
    chooser.register(envelope5.getSystemStreamPartition, null)
    // Add a bootstrap stream that's already caught up. If everything is
    // working properly, it shouldn't interfere with anything.
    chooser.register(envelope8.getSystemStreamPartition, "654")
    chooser.start
    assertNull(chooser.choose)

    // Load with a non-bootstrap stream, and should still get null.
    chooser.update(envelope3)
    assertNull(chooser.choose)

    // Load with a bootstrap stream, should get that envelope.
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)

    // Should block envelope3 since we have no message from envelope1's bootstrap stream.
    assertNull(chooser.choose)

    // Load envelope2 from non-bootstrap stream with higher priority than envelope3.
    chooser.update(envelope2)

    // Should block envelope2 since we have no message from envelope1's bootstrap stream.
    assertNull(chooser.choose)

    // Test batching by giving chooser envelope1 and envelope5, both from same stream, but envelope1 should be preferred partition.
    chooser.update(envelope5)
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)

    // Now, envelope5 is still loaded, and we've reached our batchSize limit, so loading envelope1 should still let envelope5 through.
    chooser.update(envelope1)
    assertEquals(envelope5, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertNull(chooser.choose)

    // Now we're back to just envelope3, envelope2. Let's catch up envelope1's SSP using envelope4's offset.
    chooser.update(envelope4)
    assertEquals(envelope4, chooser.choose)

    // Should still block envelopes 1 and 2 because the second partition hasn't caught up yet.
    assertNull(chooser.choose)

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
    assertNull(chooser.choose)
  }

  @Test
  def testBootstrapConfig {
    val configMap = Map(
      "task.inputs" -> "kafka.foo,kafka.bar-baz",
      "systems.kafka.streams.bar-baz.samza.bootstrap" -> "true")
    val config = new DefaultChooserConfig(new MapConfig(configMap.asJava))
    val bootstrapStreams = config.getBootstrapStreams
    assertEquals(1, bootstrapStreams.size)
    assertTrue(bootstrapStreams.contains(new SystemStream("kafka", "bar-baz")))
  }

  @Test
  def testPriorityConfig {
    val configMap = Map(
      "task.inputs" -> "kafka.foo,kafka.bar-baz",
      "systems.kafka.streams.bar-baz.samza.priority" -> "3")
    val config = new DefaultChooserConfig(new MapConfig(configMap.asJava))
    val priorityStreams = config.getPriorityStreams
    assertEquals(1, priorityStreams.size)
    assertEquals(3, priorityStreams.get(new SystemStream("kafka", "bar-baz")))
  }

  @Test
  def testBroadcastOnlyConfig {
    val configMap = Map(
      "task.broadcast.inputs" -> "kafka.foo#[1-2],kafka.bar-baz#5,kafka.fizz#0",
      "systems.kafka.streams.bar-baz.samza.priority" -> "3",
      "systems.kafka.streams.fizz.samza.bootstrap" -> "true")
    val config = new DefaultChooserConfig(new MapConfig(configMap.asJava))
    val priorityStreams = config.getPriorityStreams
    assertEquals(1, priorityStreams.size)
    assertEquals(3, priorityStreams.get(new SystemStream("kafka", "bar-baz")))

    val bootstrapStreams = config.getBootstrapStreams
    assertEquals(1, bootstrapStreams.size())
    assertTrue(bootstrapStreams.contains(new SystemStream("kafka", "fizz")))
  }

  @Test
  def testBroadcastAndStandardInputConfig {
    val configMap = Map(
      "task.broadcast.inputs" -> "kafka.foo#[1-2],kafka.bar-baz#5,kafka.fizz#0",
      "task.inputs" -> "kafka.bootstrapTopic,kafka.priorityTopic,kafka.normalTopic",
      "systems.kafka.streams.priorityTopic.samza.priority" -> "2",
      "systems.kafka.streams.bar-baz.samza.priority" -> "3",
      "systems.kafka.streams.bootstrapTopic.samza.bootstrap" -> "true",
      "systems.kafka.streams.fizz.samza.bootstrap" -> "true")
    val config = new DefaultChooserConfig(new MapConfig(configMap.asJava))
    val priorityStreams = config.getPriorityStreams
    assertEquals(2, priorityStreams.size)
    assertEquals(2, priorityStreams.get(new SystemStream("kafka", "priorityTopic")))
    assertEquals(3, priorityStreams.get(new SystemStream("kafka", "bar-baz")))

    val bootstrapStreams = config.getBootstrapStreams
    assertEquals(2, bootstrapStreams.size())
    assertTrue(bootstrapStreams.contains(new SystemStream("kafka", "bootstrapTopic")))
    assertTrue(bootstrapStreams.contains(new SystemStream("kafka", "fizz")))
  }
}

class MockBlockingEnvelopeMap extends BlockingEnvelopeMap {
  def start = Unit
  def stop = Unit
}
