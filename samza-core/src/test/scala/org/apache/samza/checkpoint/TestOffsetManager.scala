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

package org.apache.samza.checkpoint

import scala.collection.JavaConversions._
import org.apache.samza.Partition
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.SamzaException
import org.apache.samza.util.TestUtil._
import org.apache.samza.config.MapConfig
import org.apache.samza.system.SystemAdmin

class TestOffsetManager {
  @Test
  def testSystemShouldUseDefaults {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.samza.offset.default" -> "oldest"))
    val offsetManager = OffsetManager(systemStreamMetadata, config)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(!offsetManager.getLastProcessedOffset(systemStreamPartition).isDefined)
    assertTrue(offsetManager.getStartingOffset(systemStreamPartition).isDefined)
    assertEquals("0", offsetManager.getStartingOffset(systemStreamPartition).get)
  }

  @Test
  def testShouldLoadFromAndSaveWithCheckpointManager {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition)
    val systemAdmins = Map("test-system" -> getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, systemAdmins)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(partition, checkpointManager.registered.head)
    assertEquals(checkpointManager.checkpoints.head._2, checkpointManager.readLastCheckpoint(partition))
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    assertEquals("46", offsetManager.getStartingOffset(systemStreamPartition).get)
    assertEquals("45", offsetManager.getLastProcessedOffset(systemStreamPartition).get)
    offsetManager.update(systemStreamPartition, "46")
    assertEquals("46", offsetManager.getLastProcessedOffset(systemStreamPartition).get)
    offsetManager.update(systemStreamPartition, "47")
    assertEquals("47", offsetManager.getLastProcessedOffset(systemStreamPartition).get)
    // Should never update starting offset.
    assertEquals("46", offsetManager.getStartingOffset(systemStreamPartition).get)
    offsetManager.checkpoint(partition)
    val expectedCheckpoint = new Checkpoint(Map(systemStream -> "47"))
    assertEquals(expectedCheckpoint, checkpointManager.readLastCheckpoint(partition))
  }

  @Test
  def testShouldResetStreams {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val defaultOffsets = Map(systemStream -> OffsetType.OLDEST)
    val checkpoint = new Checkpoint(Map(systemStream -> "45"))
    val checkpointManager = getCheckpointManager(systemStreamPartition)
    val config = new MapConfig(Map(
      "systems.test-system.samza.offset.default" -> "oldest",
      "systems.test-system.streams.test-stream.samza.reset.offset" -> "true"))
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager)
    offsetManager.register(systemStreamPartition)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(partition, checkpointManager.registered.head)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(partition))
    // Should be zero even though the checkpoint has an offset of 45, since we're forcing a reset.
    assertEquals("0", offsetManager.getStartingOffset(systemStreamPartition).get)
  }

  @Test
  def testOffsetManagerShouldHandleNullCheckpoints {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition1 = new Partition(0)
    val partition2 = new Partition(1)
    val systemStreamPartition1 = new SystemStreamPartition(systemStream, partition1)
    val systemStreamPartition2 = new SystemStreamPartition(systemStream, partition2)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(
      partition1 -> new SystemStreamPartitionMetadata("0", "1", "2"),
      partition2 -> new SystemStreamPartitionMetadata("3", "4", "5")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val defaultOffsets = Map(systemStream -> OffsetType.OLDEST)
    val checkpoint = new Checkpoint(Map(systemStream -> "45"))
    // Checkpoint manager only has partition 1.
    val checkpointManager = getCheckpointManager(systemStreamPartition1)
    val systemAdmins = Map("test-system" -> getSystemAdmin)
    val config = new MapConfig
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, systemAdmins)
    // Register both partitions. Partition 2 shouldn't have a checkpoint.
    offsetManager.register(systemStreamPartition1)
    offsetManager.register(systemStreamPartition2)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(2, checkpointManager.registered.size)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(partition1))
    assertNull(checkpointManager.readLastCheckpoint(partition2))
  }

  @Test
  def testShouldFailWhenMissingMetadata {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val offsetManager = new OffsetManager
    offsetManager.register(systemStreamPartition)

    expect(classOf[SamzaException], Some("Attempting to load defaults for stream SystemStream [system=test-system, stream=test-stream], which has no offset settings.")) {
      offsetManager.start
    }
  }

  @Test
  def testShouldFailWhenMissingDefault {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val offsetManager = OffsetManager(systemStreamMetadata, new MapConfig(Map[String, String]()))
    offsetManager.register(systemStreamPartition)

    expect(classOf[SamzaException], Some("No default offeset defined for SystemStream [system=test-system, stream=test-stream]. Unable to load a default.")) {
      offsetManager.start
    }
  }

  @Test
  def testDefaultSystemShouldFailWhenFailIsSpecified {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.samza.offset.default" -> "fail"))
    expect(classOf[IllegalArgumentException]) {
      OffsetManager(systemStreamMetadata, config)
    }
  }

  @Test
  def testDefaultStreamShouldFailWhenFailIsSpecified {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.streams.test-stream.samza.offset.default" -> "fail"))
    expect(classOf[IllegalArgumentException]) {
      OffsetManager(systemStreamMetadata, config)
    }
  }

  @Test
  def testOutdatedStreamInCheckpoint {
    val systemStream0 = new SystemStream("test-system-0", "test-stream")
    val systemStream1 = new SystemStream("test-system-1", "test-stream")
    val partition0 = new Partition(0)
    val systemStreamPartition0 = new SystemStreamPartition(systemStream0, partition0)
    val systemStreamPartition1 = new SystemStreamPartition(systemStream1, partition0)
    val testStreamMetadata = new SystemStreamMetadata(systemStream0.getStream, Map(partition0 -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val systemStreamMetadata = Map(systemStream0 -> testStreamMetadata)
    val offsetSettings = Map(systemStream0 -> OffsetSetting(testStreamMetadata, OffsetType.UPCOMING, false))
    val checkpointManager = getCheckpointManager(systemStreamPartition1)
    val offsetManager = new OffsetManager(offsetSettings, checkpointManager)
    offsetManager.register(systemStreamPartition0)
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertNull(offsetManager.getLastProcessedOffset(systemStreamPartition1).getOrElse(null))
  }

  private def getCheckpointManager(systemStreamPartition: SystemStreamPartition) = {
    val checkpoint = new Checkpoint(Map(systemStreamPartition.getSystemStream -> "45"))

    new CheckpointManager {
      var isStarted = false
      var isStopped = false
      var registered = Set[Partition]()
      var checkpoints = Map(systemStreamPartition.getPartition -> checkpoint)
      def start { isStarted = true }
      def register(partition: Partition) { registered += partition }
      def writeCheckpoint(partition: Partition, checkpoint: Checkpoint) { checkpoints += partition -> checkpoint }
      def readLastCheckpoint(partition: Partition) = checkpoints.getOrElse(partition, null)
      def stop { isStopped = true }
    }
  }

  private def getSystemAdmin = {
    new SystemAdmin {
      def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) =
        offsets.mapValues(offset => (offset.toLong + 1).toString)

      def getSystemStreamMetadata(streamNames: java.util.Set[String]) =
        Map[String, SystemStreamMetadata]()
    }
  }
}