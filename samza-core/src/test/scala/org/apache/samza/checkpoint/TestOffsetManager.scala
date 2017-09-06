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

import java.util
import java.util.Collections
import java.util.Collections.EmptyMap

import org.apache.samza.container.TaskName
import org.apache.samza.Partition
import org.apache.samza.system._
import org.apache.samza.system.SystemStreamMetadata.{OffsetType, SystemStreamPartitionMetadata}
import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.apache.samza.SamzaException
import org.apache.samza.config.MapConfig
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class TestOffsetManager {
  @Test
  def testSystemShouldUseDefaults {
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.samza.offset.default" -> "oldest").asJava)
    val offsetManager = OffsetManager(systemStreamMetadata, config)
    offsetManager.register(taskName, Set(systemStreamPartition))
    offsetManager.start
    assertFalse(offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).isDefined)
    assertTrue(offsetManager.getStartingOffset(taskName, systemStreamPartition).isDefined)
    assertEquals("0", offsetManager.getStartingOffset(taskName, systemStreamPartition).get)
  }

  @Test
  def testShouldLoadFromAndSaveWithCheckpointManager {
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName)
    val systemAdmins = Map("test-system" -> getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition))
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(taskName, checkpointManager.registered.head)
    assertEquals(checkpointManager.checkpoints.head._2, checkpointManager.readLastCheckpoint(taskName))
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    assertEquals("46", offsetManager.getStartingOffset(taskName, systemStreamPartition).get)
    assertEquals("45", offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).get)
    offsetManager.update(taskName, systemStreamPartition, "46")
    assertEquals("46", offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).get)
    offsetManager.update(taskName, systemStreamPartition, "47")
    assertEquals("47", offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).get)
    // Should never update starting offset.
    assertEquals("46", offsetManager.getStartingOffset(taskName, systemStreamPartition).get)
    // Should not update null offset
    offsetManager.update(taskName, systemStreamPartition, null)
    offsetManager.checkpoint(taskName)
    val expectedCheckpoint = new Checkpoint(Map(systemStreamPartition -> "47").asJava)
    assertEquals(expectedCheckpoint, checkpointManager.readLastCheckpoint(taskName))
  }

  @Test
  def testGetCheckpointedOffsetMetric{
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName)
    val systemAdmins = Map("test-system" -> getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition))
    offsetManager.start
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    offsetManager.checkpoint(taskName)
    assertEquals("45", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    offsetManager.update(taskName, systemStreamPartition, "46")
    offsetManager.update(taskName, systemStreamPartition, "47")
    offsetManager.checkpoint(taskName)
    assertEquals("47", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    offsetManager.update(taskName, systemStreamPartition, "48")
    offsetManager.checkpoint(taskName)
    assertEquals("48", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
  }

  @Test
  def testShouldResetStreams {
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val checkpoint = new Checkpoint(Map(systemStreamPartition -> "45").asJava)
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName)
    val config = new MapConfig(Map(
      "systems.test-system.samza.offset.default" -> "oldest",
      "systems.test-system.streams.test-stream.samza.reset.offset" -> "true").asJava)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager)
    offsetManager.register(taskName, Set(systemStreamPartition))
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertEquals(taskName, checkpointManager.registered.head)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(taskName))
    // Should be zero even though the checkpoint has an offset of 45, since we're forcing a reset.
    assertEquals("0", offsetManager.getStartingOffset(taskName, systemStreamPartition).get)
  }

  @Test
  def testOffsetManagerShouldHandleNullCheckpoints {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition1 = new Partition(0)
    val partition2 = new Partition(1)
    val taskName1 = new TaskName("P0")
    val taskName2 = new TaskName("P1")
    val systemStreamPartition1 = new SystemStreamPartition(systemStream, partition1)
    val systemStreamPartition2 = new SystemStreamPartition(systemStream, partition2)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(
      partition1 -> new SystemStreamPartitionMetadata("0", "1", "2"),
      partition2 -> new SystemStreamPartitionMetadata("3", "4", "5")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val checkpoint = new Checkpoint(Map(systemStreamPartition1 -> "45").asJava)
    // Checkpoint manager only has partition 1.
    val checkpointManager = getCheckpointManager(systemStreamPartition1, taskName1)
    val systemAdmins = Map("test-system" -> getSystemAdmin)
    val config = new MapConfig
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, systemAdmins)
    // Register both partitions. Partition 2 shouldn't have a checkpoint.
    offsetManager.register(taskName1, Set(systemStreamPartition1))
    offsetManager.register(taskName2, Set(systemStreamPartition2))
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(2, checkpointManager.registered.size)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(taskName1))
    assertNull(checkpointManager.readLastCheckpoint(taskName2))
  }

  @Test
  def testShouldFailWhenMissingMetadata {
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val offsetManager = new OffsetManager
    offsetManager.register(taskName, Set(systemStreamPartition))

    intercept[SamzaException] {
      offsetManager.start
    }
  }

  @Test
  def testDefaultSystemShouldFailWhenFailIsSpecified {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.samza.offset.default" -> "fail").asJava)
    intercept[IllegalArgumentException] {
      OffsetManager(systemStreamMetadata, config)
    }
  }

  @Test
  def testDefaultStreamShouldFailWhenFailIsSpecified {
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig(Map("systems.test-system.streams.test-stream.samza.offset.default" -> "fail").asJava)

    intercept[IllegalArgumentException] {
      OffsetManager(systemStreamMetadata, config)
    }
  }

  @Test
  def testOutdatedStreamInCheckpoint {
    val taskName = new TaskName("c")
    val systemStream0 = new SystemStream("test-system-0", "test-stream")
    val systemStream1 = new SystemStream("test-system-1", "test-stream")
    val partition0 = new Partition(0)
    val systemStreamPartition0 = new SystemStreamPartition(systemStream0, partition0)
    val systemStreamPartition1 = new SystemStreamPartition(systemStream1, partition0)
    val testStreamMetadata = new SystemStreamMetadata(systemStream0.getStream, Map(partition0 -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val offsetSettings = Map(systemStream0 -> OffsetSetting(testStreamMetadata, OffsetType.UPCOMING, false))
    val checkpointManager = getCheckpointManager(systemStreamPartition1)
    val offsetManager = new OffsetManager(offsetSettings, checkpointManager)
    offsetManager.register(taskName, Set(systemStreamPartition0))
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(1, checkpointManager.registered.size)
    assertNull(offsetManager.getLastProcessedOffset(taskName, systemStreamPartition1).getOrElse(null))
  }

  @Test
  def testDefaultToUpcomingOnMissingDefault {
    val taskName = new TaskName("task-name")
    val ssp = new SystemStreamPartition(new SystemStream("test-system", "test-stream"), new Partition(0))
    val sspm = new SystemStreamPartitionMetadata(null, null, "13")
    val offsetMeta = new SystemStreamMetadata("test-stream", Map(new Partition(0) -> sspm).asJava)
    val settings = new OffsetSetting(offsetMeta, OffsetType.OLDEST, resetOffset = false)
    val offsetManager = new OffsetManager(offsetSettings = Map(ssp.getSystemStream -> settings))
    offsetManager.register(taskName, Set(ssp))
    offsetManager.start
    assertEquals(Some("13"), offsetManager.getStartingOffset(taskName, ssp))
  }

  @Test
  def testCheckpointListener{
    val taskName = new TaskName("c")
    val systemName = "test-system"
    val systemName2 = "test-system2"
    val systemStream = new SystemStream(systemName, "test-stream")
    val systemStream2 = new SystemStream(systemName2, "test-stream2")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val systemStreamPartition2 = new SystemStreamPartition(systemStream2, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val testStreamMetadata2 = new SystemStreamMetadata(systemStream2.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata, systemStream2->testStreamMetadata2)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager1(systemStreamPartition,
                                                 new Checkpoint(Map(systemStreamPartition -> "45", systemStreamPartition2 -> "100").asJava),
                                                 taskName)
    val systemAdmins = Map(systemName -> getSystemAdmin, systemName2->getSystemAdmin)
    val consumer = new SystemConsumerWithCheckpointCallback

    val checkpointListeners: Map[String, CheckpointListener] = if(consumer.isInstanceOf[CheckpointListener])
      Map(systemName -> consumer.asInstanceOf[CheckpointListener])
    else
      Map()

    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager,
                                      systemAdmins, checkpointListeners, new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition, systemStreamPartition2))

    offsetManager.start
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    offsetManager.checkpoint(taskName)
    assertEquals("45", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("100", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("45", consumer.recentCheckpoint.get(systemStreamPartition))
    // make sure only the system with the callbacks gets them
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))

    offsetManager.update(taskName, systemStreamPartition, "46")
    offsetManager.update(taskName, systemStreamPartition, "47")
    offsetManager.checkpoint(taskName)
    assertEquals("47", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("100", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("47", consumer.recentCheckpoint.get(systemStreamPartition))
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))

    offsetManager.update(taskName, systemStreamPartition, "48")
    offsetManager.update(taskName, systemStreamPartition2, "101")
    offsetManager.checkpoint(taskName)
    assertEquals("48", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("101", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("48", consumer.recentCheckpoint.get(systemStreamPartition))
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))
    offsetManager.stop
  }

  class SystemConsumerWithCheckpointCallback extends SystemConsumer with CheckpointListener{
    var recentCheckpoint: java.util.Map[SystemStreamPartition, String] = java.util.Collections.emptyMap[SystemStreamPartition, String]
    override def start() {}

    override def stop() {}

    override def register(systemStreamPartition: SystemStreamPartition, offset: String) {}

    override def poll(systemStreamPartitions: util.Set[SystemStreamPartition],
                      timeout: Long): util.Map[SystemStreamPartition, util.List[IncomingMessageEnvelope]] = { null }

    override def onCheckpoint(offsets: java.util.Map[SystemStreamPartition,String]){
      recentCheckpoint = (recentCheckpoint.asScala ++ offsets.asScala).asJava
    }
  }

  private def getCheckpointManager(systemStreamPartition: SystemStreamPartition, taskName:TaskName = new TaskName("taskName")) = {
    getCheckpointManager1(systemStreamPartition, new Checkpoint(Map(systemStreamPartition -> "45").asJava), taskName)
  }

  private def getCheckpointManager1(systemStreamPartition: SystemStreamPartition, checkpoint: Checkpoint, taskName:TaskName = new TaskName("taskName")) = {
    new CheckpointManager {
      var isStarted = false
      var isStopped = false
      var registered = Set[TaskName]()
      var checkpoints: Map[TaskName, Checkpoint] = Map(taskName -> checkpoint)
      var taskNameToPartitionMapping: util.Map[TaskName, java.lang.Integer] = new util.HashMap[TaskName, java.lang.Integer]()
      def start { isStarted = true }
      def register(taskName: TaskName) { registered += taskName }
      def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) { checkpoints += taskName -> checkpoint }
      def readLastCheckpoint(taskName: TaskName) = checkpoints.getOrElse(taskName, null)
      def stop { isStopped = true }

      // Only for testing purposes - not present in actual checkpoint manager
      def getOffets = Map(taskName -> checkpoint.getOffsets.asScala.toMap)
    }
  }

  private def getSystemAdmin = {
    new SystemAdmin {
      def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) =
        offsets.asScala.mapValues(offset => (offset.toLong + 1).toString).asJava

      def getSystemStreamMetadata(streamNames: java.util.Set[String]) =
        Map[String, SystemStreamMetadata]().asJava

      override def offsetComparator(offset1: String, offset2: String) = null
    }
  }
}
