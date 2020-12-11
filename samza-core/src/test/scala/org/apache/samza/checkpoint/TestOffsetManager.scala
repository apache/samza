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
import java.util.function.BiConsumer

import com.google.common.collect.ImmutableMap
import org.apache.samza.config.MapConfig
import org.apache.samza.container.TaskName
import org.apache.samza.startpoint.{Startpoint, StartpointManagerTestUtil, StartpointOldest, StartpointSpecific, StartpointUpcoming}
import org.apache.samza.system.SystemStreamMetadata.{OffsetType, SystemStreamPartitionMetadata}
import org.apache.samza.system._
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers._
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Matchers, Mockito}
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
    val startpointManagerUtil = getStartpointManagerUtil
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition))
    startpointManagerUtil.getStartpointManager.writeStartpoint(systemStreamPartition, taskName, new StartpointOldest)
    assertTrue(startpointManagerUtil.getStartpointManager.readStartpoint(systemStreamPartition, taskName).isPresent)
    assertTrue(startpointManagerUtil.getStartpointManager.fanOut(asTaskToSSPMap(taskName, systemStreamPartition)).keySet().contains(taskName))
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
    assertTrue(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition))
    checkpoint(offsetManager, taskName)
    intercept[IllegalStateException] {
      // StartpointManager should stop after last fan out is removed
      startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName)
    }
    startpointManagerUtil.getStartpointManager.start
    assertFalse(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition)) // Startpoint should delete after checkpoint commit
    val expectedCheckpoint = new Checkpoint(Map(systemStreamPartition -> "47").asJava)
    assertEquals(expectedCheckpoint, checkpointManager.readLastCheckpoint(taskName))
    startpointManagerUtil.stop
  }

  @Test
  def testGetAndSetStartpoint {
    val taskName1 = new TaskName("c")
    val taskName2 = new TaskName("d")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName1)
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)

    offsetManager.register(taskName1, Set(systemStreamPartition))
    val startpoint1 = new StartpointOldest
    startpointManagerUtil.getStartpointManager.writeStartpoint(systemStreamPartition, taskName1, startpoint1)
    assertTrue(startpointManagerUtil.getStartpointManager.readStartpoint(systemStreamPartition, taskName1).isPresent)
    assertTrue(startpointManagerUtil.getStartpointManager.fanOut(asTaskToSSPMap(taskName1, systemStreamPartition)).keySet().contains(taskName1))
    offsetManager.start
    val startpoint2 = new StartpointUpcoming
    offsetManager.setStartpoint(taskName2, systemStreamPartition, startpoint2)

    assertEquals(Option(startpoint1), offsetManager.getStartpoint(taskName1, systemStreamPartition))
    assertEquals(Option(startpoint2), offsetManager.getStartpoint(taskName2, systemStreamPartition))
    startpointManagerUtil.stop
  }

  @Test
  def testGetStartingOffsetWhenResolvedFromStartpoint: Unit = {
    val taskName1 = new TaskName("c")
    val taskName2 = new TaskName("d")
    val systemStream1 = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream1, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream1.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "51", "52")).asJava)
    val systemStreamMetadata = Map(systemStream1 -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName1)
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    val systemAdmin = mock(classOf[SystemAdmin])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(systemAdmin)
    val testStartpoint = new StartpointSpecific("23")
    Mockito.doReturn(testStartpoint.getSpecificOffset).when(systemAdmin).resolveStartpointToOffset(refEq(systemStreamPartition), refEq(testStartpoint))
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)

    offsetManager.register(taskName1, Set(systemStreamPartition))
    val startpointManager = startpointManagerUtil.getStartpointManager
    startpointManager.writeStartpoint(systemStreamPartition, testStartpoint)
    startpointManager.fanOut(asTaskToSSPMap(taskName1, systemStreamPartition))
    offsetManager.start
    assertEquals(testStartpoint.getSpecificOffset, offsetManager.getStartingOffset(taskName1, systemStreamPartition).get)
  }

  @Test
  def testGetStartingOffsetWhenResolveStartpointToOffsetIsNull: Unit = {
    val taskName1 = new TaskName("c")
    val taskName2 = new TaskName("d")
    val systemStream1 = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream1, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream1.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "51", "52")).asJava)
    val systemStreamMetadata = Map(systemStream1 -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName1)
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    val systemAdmin = mock(classOf[SystemAdmin])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(systemAdmin)
    Mockito.doReturn(null).when(systemAdmin).resolveStartpointToOffset(refEq(systemStreamPartition), refEq(null))
    Mockito.doReturn(ImmutableMap.of(systemStreamPartition, "46")).when(systemAdmin).getOffsetsAfter(any[util.Map[SystemStreamPartition, String]])
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName1, Set(systemStreamPartition))
    val startpointManager = startpointManagerUtil.getStartpointManager
    offsetManager.start
    assertEquals("46", offsetManager.getStartingOffset(taskName1, systemStreamPartition).get)
  }

  @Test
  def testGetStartingOffsetWhenResolveStartpointToOffsetThrows: Unit = {
    val taskName1 = new TaskName("c")
    val taskName2 = new TaskName("d")
    val systemStream1 = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream1, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream1.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "51", "52")).asJava)
    val systemStreamMetadata = Map(systemStream1 -> testStreamMetadata)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName1)
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    val systemAdmin = mock(classOf[SystemAdmin])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(systemAdmin)
    Mockito.doThrow(new RuntimeException("mock startpoint resolution exception")).when(systemAdmin).resolveStartpointToOffset(refEq(systemStreamPartition), refEq(null))
    Mockito.doReturn(ImmutableMap.of(systemStreamPartition, "46")).when(systemAdmin).getOffsetsAfter(any[util.Map[SystemStreamPartition, String]])
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName1, Set(systemStreamPartition))
    val startpointManager = startpointManagerUtil.getStartpointManager
    offsetManager.start
    assertEquals("46", offsetManager.getStartingOffset(taskName1, systemStreamPartition).get)
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
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition))

    // Pre-populate startpoint
    startpointManagerUtil.getStartpointManager.writeStartpoint(systemStreamPartition, taskName, new StartpointOldest)
    assertTrue(startpointManagerUtil.getStartpointManager.fanOut(asTaskToSSPMap(taskName, systemStreamPartition)).keySet().contains(taskName))
    offsetManager.start
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    assertTrue(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition))
    checkpoint(offsetManager, taskName)
    intercept[IllegalStateException] {
      // StartpointManager should stop after last fan out is removed
      startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName)
    }
    startpointManagerUtil.getStartpointManager.start
    assertFalse(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition)) // Startpoint should delete after checkpoint commit
    assertEquals("45", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    offsetManager.update(taskName, systemStreamPartition, "46")

    offsetManager.update(taskName, systemStreamPartition, "47")
    checkpoint(offsetManager, taskName)
    assertEquals("47", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)

    offsetManager.update(taskName, systemStreamPartition, "48")
    checkpoint(offsetManager, taskName)
    assertEquals("48", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    startpointManagerUtil.stop
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
    val startpointManagerUtil = getStartpointManagerUtil()

    val config = new MapConfig
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins)
    // Register both partitions. Partition 2 shouldn't have a checkpoint.
    offsetManager.register(taskName1, Set(systemStreamPartition1))
    offsetManager.register(taskName2, Set(systemStreamPartition2))
    offsetManager.start
    assertTrue(checkpointManager.isStarted)
    assertEquals(2, checkpointManager.registered.size)
    assertEquals(checkpoint, checkpointManager.readLastCheckpoint(taskName1))
    assertNull(checkpointManager.readLastCheckpoint(taskName2))
    startpointManagerUtil.stop
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
    val systemStream3 = new SystemStream(systemName, "test-stream3")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val systemStreamPartition2 = new SystemStreamPartition(systemStream2, partition)
    val unregisteredSystemStreamPartition = new SystemStreamPartition(systemStream3, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val testStreamMetadata2 = new SystemStreamMetadata(systemStream2.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata, systemStream2->testStreamMetadata2)
    val config = new MapConfig
    val checkpointManager = getCheckpointManager1(new Checkpoint(Map(systemStreamPartition -> "45", systemStreamPartition2 -> "100").asJava),
                                                 taskName)
    val startpointManagerUtil = getStartpointManagerUtil()
    val consumer = new SystemConsumerWithCheckpointCallback
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin(systemName)).thenReturn(getSystemAdmin)
    when(systemAdmins.getSystemAdmin(systemName2)).thenReturn(getSystemAdmin)

    val checkpointListeners: Map[String, CheckpointListener] = if(consumer.isInstanceOf[CheckpointListener])
      Map(systemName -> consumer.asInstanceOf[CheckpointListener])
    else
      Map()

    val offsetManager = OffsetManager(systemStreamMetadata, config, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins,
      checkpointListeners, new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition, systemStreamPartition2))

    startpointManagerUtil.getStartpointManager.writeStartpoint(systemStreamPartition, taskName, new StartpointOldest)
    assertTrue(startpointManagerUtil.getStartpointManager.readStartpoint(systemStreamPartition, taskName).isPresent)
    assertTrue(startpointManagerUtil.getStartpointManager.fanOut(asTaskToSSPMap(taskName, systemStreamPartition)).keySet().contains(taskName))
    assertFalse(startpointManagerUtil.getStartpointManager.readStartpoint(systemStreamPartition, taskName).isPresent)
    offsetManager.start
    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    assertTrue(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition))
    val offsetsToCheckpoint = new java.util.HashMap[SystemStreamPartition, String]()
    offsetsToCheckpoint.putAll(offsetManager.buildCheckpoint(taskName).getInputOffsets)
    offsetsToCheckpoint.put(unregisteredSystemStreamPartition, "50")
    offsetManager.writeCheckpoint(taskName, new Checkpoint(offsetsToCheckpoint))

    intercept[IllegalStateException] {
      // StartpointManager should stop after last fan out is removed
      startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName)
    }
    startpointManagerUtil.getStartpointManager.start
    assertFalse(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition)) // Startpoint be deleted at first checkpoint

    assertEquals("45", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("100", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("45", consumer.recentCheckpoint.get(systemStreamPartition))
    // make sure only the system with the callbacks gets them
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))
    // even though systemStream and systemStream3 share the same checkpointListener, callback should not execute for
    // systemStream3 since it is not registered with the OffsetManager
    assertNull(consumer.recentCheckpoint.get(unregisteredSystemStreamPartition))

    offsetManager.update(taskName, systemStreamPartition, "46")
    offsetManager.update(taskName, systemStreamPartition, "47")
    checkpoint(offsetManager, taskName)
    assertEquals("47", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("100", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("47", consumer.recentCheckpoint.get(systemStreamPartition))
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))

    offsetManager.update(taskName, systemStreamPartition, "48")
    offsetManager.update(taskName, systemStreamPartition2, "101")
    checkpoint(offsetManager, taskName)
    assertEquals("48", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
    assertEquals("101", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition2).getValue)
    assertEquals("48", consumer.recentCheckpoint.get(systemStreamPartition))
    assertNull(consumer.recentCheckpoint.get(systemStreamPartition2))
    offsetManager.stop
    startpointManagerUtil.stop
  }

  /**
    * If task.max.concurrency > 1 and task.async.commit == true, a task could update its offsets at the same time as
    * TaskInstance.commit(). This makes it possible to checkpoint offsets which did not successfully flush.
    *
    * This is prevented by using separate methods to get a checkpoint and write that checkpoint. See SAMZA-1384
    */
  @Test
  def testConcurrentCheckpointAndUpdate{
    val taskName = new TaskName("c")
    val systemStream = new SystemStream("test-system", "test-stream")
    val partition = new Partition(0)
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val testStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")).asJava)
    val systemStreamMetadata = Map(systemStream -> testStreamMetadata)
    val checkpointManager = getCheckpointManager(systemStreamPartition, taskName)
    val startpointManagerUtil = getStartpointManagerUtil()
    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin("test-system")).thenReturn(getSystemAdmin)
    val offsetManager = OffsetManager(systemStreamMetadata, new MapConfig, checkpointManager, startpointManagerUtil.getStartpointManager, systemAdmins, Map(), new OffsetManagerMetrics)
    offsetManager.register(taskName, Set(systemStreamPartition))
    startpointManagerUtil.getStartpointManager.writeStartpoint(systemStreamPartition, taskName, new StartpointOldest)
    assertTrue(startpointManagerUtil.getStartpointManager.fanOut(asTaskToSSPMap(taskName, systemStreamPartition)).keySet().contains(taskName))
    offsetManager.start

    // Should get offset 45 back from the checkpoint manager, which is last processed, and system admin should return 46 as starting offset.
    assertTrue(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition))
    checkpoint(offsetManager, taskName)
    intercept[IllegalStateException] {
      // StartpointManager should stop after last fan out is removed
      startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName)
    }
    startpointManagerUtil.getStartpointManager.start
    assertFalse(startpointManagerUtil.getStartpointManager.getFanOutForTask(taskName).containsKey(systemStreamPartition)) // Startpoint be deleted at first checkpoint
    assertEquals("45", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)

    offsetManager.update(taskName, systemStreamPartition, "46")
    // Get checkpoint snapshot like we do at the beginning of TaskInstance.commit()
    val checkpoint46 = offsetManager.buildCheckpoint(taskName)
    offsetManager.update(taskName, systemStreamPartition, "47") // Offset updated before checkpoint
    offsetManager.writeCheckpoint(taskName, checkpoint46)
    assertEquals(Some("47"), offsetManager.getLastProcessedOffset(taskName, systemStreamPartition))
    assertEquals("46", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)

    // Now write the checkpoint for the latest offset
    val checkpoint47 = offsetManager.buildCheckpoint(taskName)
    offsetManager.writeCheckpoint(taskName, checkpoint47)
    startpointManagerUtil.stop
    assertEquals(Some("47"), offsetManager.getLastProcessedOffset(taskName, systemStreamPartition))
    assertEquals("47", offsetManager.offsetManagerMetrics.checkpointedOffsets.get(systemStreamPartition).getValue)
  }

  @Test
  def testGetModifiedOffsets: Unit = {
    val system1 = "system1"
    val system2 = "system2"
    val ssp1 = new SystemStreamPartition(system1, "stream", new Partition(1))
    val ssp2 = new SystemStreamPartition(system1, "stream1", new Partition(1))
    val ssp3 = new SystemStreamPartition(system1, "stream1", new Partition(2))
    val ssp4 = new SystemStreamPartition(system1, "stream1", new Partition(3))
    val ssp5 = new SystemStreamPartition(system2, "stream", new Partition(1))
    val ssp6 = new SystemStreamPartition(system2, "stream1", new Partition(1))

    val lastProcessedOffsets = Map(ssp1 -> "10", ssp2 -> "20", ssp3 -> "30", ssp4 -> "40", ssp5 -> "50", ssp6 -> "60")
    // starting offsets are (lastProcessedOffset + 1) (i.e. checkpointed offset + 1) on startup
    val taskStartingOffsets = Map(ssp1 -> "11", ssp2 -> "19", ssp3 -> null, ssp5 -> "51", ssp6 -> "59")

    // test behavior without any checkpoint listeners
    val offsetManager = new OffsetManager()
    val regularOffsets = offsetManager.getModifiedOffsets(taskStartingOffsets, lastProcessedOffsets)
    // since there are no checkpoint listeners, there should be no change in offsets.
    assertEquals("10", regularOffsets.get(ssp1))
    assertEquals("20", regularOffsets.get(ssp2))
    assertEquals("30", regularOffsets.get(ssp3))
    assertEquals("40", regularOffsets.get(ssp4))
    assertEquals("50", regularOffsets.get(ssp5))
    assertEquals("60", regularOffsets.get(ssp6))

    // test behavior with a checkpoint listener for "system1" that increments all provided offsets by 5
    val checkpointListener: CheckpointListener = new CheckpointListener {
      override def beforeCheckpoint(offsets: util.Map[SystemStreamPartition, String]) = {
        val results = new util.HashMap[SystemStreamPartition, String]()
        offsets.forEach(new BiConsumer[SystemStreamPartition, String] {
          override def accept(ssp: SystemStreamPartition, offset: String): Unit = {
            results.put(ssp, (offset.toLong + 5).toString)
          }
        })
        results
      }
    }
    val checkpointListeners = Map(system1 -> checkpointListener)

    val system1Admin = mock(classOf[SystemAdmin])
    Mockito.when(system1Admin.offsetComparator(anyString(), anyString()))
      .thenAnswer(new Answer[Integer] {
          override def answer(invocation: InvocationOnMock): Integer = {
            val offset1 = invocation.getArguments.apply(0).asInstanceOf[String]
            val offset2 = invocation.getArguments.apply(1).asInstanceOf[String]
            offset1.toLong.compareTo(offset2.toLong)
          }
        })

    val systemAdmins = mock(classOf[SystemAdmins])
    when(systemAdmins.getSystemAdmin(Matchers.eq("system1"))).thenReturn(system1Admin)

    val offsetManagerWithCheckpointListener =
      new OffsetManager(checkpointListeners = checkpointListeners, systemAdmins = systemAdmins)
    val modifiedOffsets = offsetManagerWithCheckpointListener.getModifiedOffsets(taskStartingOffsets, lastProcessedOffsets)
    // since there is at least one ssp on system1 that has processed messages (ssp2),
    // all ssps on system1 should get modified offsets (ssp1 to ssp4)
    assertEquals("15", modifiedOffsets.get(ssp1))
    assertEquals("25", modifiedOffsets.get(ssp2))
    assertEquals("35", modifiedOffsets.get(ssp3))
    assertEquals("45", modifiedOffsets.get(ssp4))
    // no change for ssps on system2
    assertEquals("50", modifiedOffsets.get(ssp5))
    assertEquals("60", modifiedOffsets.get(ssp6))
  }

  // Utility method to create and write checkpoint in one statement
  def checkpoint(offsetManager: OffsetManager, taskName: TaskName): Unit = {
    offsetManager.writeCheckpoint(taskName, offsetManager.buildCheckpoint(taskName))
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
    getCheckpointManager1(new Checkpoint(Map(systemStreamPartition -> "45").asJava), taskName)
  }

  private def getCheckpointManager1(checkpoint: Checkpoint, taskName:TaskName = new TaskName("taskName")) = {
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
      def getOffets = Map(taskName -> checkpoint.getInputOffsets.asScala.toMap)
    }
  }

  private def getStartpointManagerUtil() = {
    val startpointManagerUtil = new StartpointManagerTestUtil
    startpointManagerUtil
  }

  private def getSystemAdmin: SystemAdmin = {
    new SystemAdmin {
      def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) =
        offsets.asScala.mapValues(offset => (offset.toLong + 1).toString).asJava

      def getSystemStreamMetadata(streamNames: java.util.Set[String]) =
        Map[String, SystemStreamMetadata]().asJava

      override def offsetComparator(offset1: String, offset2: String) = null
    }
  }

  private def asTaskToSSPMap(taskName: TaskName, ssps: SystemStreamPartition*) = {
    Map(taskName -> ssps.toSet.asJava).asJava
  }
}
