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

import org.apache.samza.Partition
import org.apache.samza.checkpoint.CheckpointTool.CheckpointToolCommandLine
import org.apache.samza.container.TaskName
import org.apache.samza.checkpoint.TestCheckpointTool.{MockCheckpointManagerFactory, MockSystemFactory}
import org.apache.samza.config.{Config, MapConfig, SystemConfig, TaskConfig}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{SystemAdmin, SystemConsumer, SystemFactory, SystemProducer, SystemStreamMetadata, SystemStreamPartition}
import org.junit.{Before, Test}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar
import scala.collection.JavaConverters._
import org.apache.samza.config.JobConfig
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory

import scala.collection.immutable.HashMap

object TestCheckpointTool {
  var checkpointManager: CheckpointManager = null
  var systemConsumer: SystemConsumer = null
  var systemProducer: SystemProducer = null
  var systemAdmin: SystemAdmin = null

  class MockCheckpointManagerFactory extends CheckpointManagerFactory {
    def getCheckpointManager(config: Config, registry: MetricsRegistry) = checkpointManager
  }

  class MockSystemFactory extends SystemFactory {
    override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = systemConsumer
    override def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = systemProducer
    override def getAdmin(systemName: String, config: Config) = systemAdmin
  }
}

class TestCheckpointTool extends AssertionsForJUnit with MockitoSugar {
  var config: MapConfig = null

  val tn0 = new TaskName("Partition 0")
  val tn1 = new TaskName("Partition 1")
  val p0 = new Partition(0)
  val p1 = new Partition(1)

  @Before
  def setup {
    config = new MapConfig(Map(
      JobConfig.JOB_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      TaskConfig.INPUT_STREAMS -> "test.foo",
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getName,
      SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getName,
      SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName,
      TaskConfig.GROUPER_FACTORY -> "org.apache.samza.container.grouper.task.GroupByContainerCountFactory"
    ).asJava)
    val metadata = new SystemStreamMetadata("foo", Map[Partition, SystemStreamPartitionMetadata](
      new Partition(0) -> new SystemStreamPartitionMetadata("0", "100", "101"),
      new Partition(1) -> new SystemStreamPartitionMetadata("0", "200", "201")
    ).asJava)
    TestCheckpointTool.checkpointManager = mock[CheckpointManager]
    TestCheckpointTool.systemAdmin = mock[SystemAdmin]
    when(TestCheckpointTool.systemAdmin.getSystemStreamMetadata(Set("foo").asJava))
      .thenReturn(Map("foo" -> metadata).asJava)
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn0))
      .thenReturn(new Checkpoint(Map(new SystemStreamPartition("test", "foo", p0) -> "1234").asJava))
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn1))
      .thenReturn(new Checkpoint(Map(new SystemStreamPartition("test", "foo", p1) -> "4321").asJava))
  }

  @Test
  def testReadLatestCheckpoint {
    val checkpointTool = CheckpointTool(config, null)
    checkpointTool.run
    verify(TestCheckpointTool.checkpointManager).readLastCheckpoint(tn0)
    verify(TestCheckpointTool.checkpointManager).readLastCheckpoint(tn1)
    verify(TestCheckpointTool.checkpointManager, never()).writeCheckpoint(any(), any())
  }

  @Test
  def testOverwriteCheckpoint {
    val toOverwrite = Map(tn0 -> Map(new SystemStreamPartition("test", "foo", p0) -> "42"),
      tn1 -> Map(new SystemStreamPartition("test", "foo", p1) -> "43"))

    val checkpointTool = CheckpointTool(config, toOverwrite)
    checkpointTool.run
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn0, new Checkpoint(Map(new SystemStreamPartition("test", "foo", p0) -> "42").asJava))
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn1, new Checkpoint(Map(new SystemStreamPartition("test", "foo", p1) -> "43").asJava))
  }

  @Test
  def testGrouping(): Unit = {
    val config : java.util.Map[String, String] = new util.HashMap()
    config.put("tasknames.Partition 0.systems.kafka-atc-repartitioned-requests.streams.ArticleRead.partitions.0", "0000")
    config.put("tasknames.Partition 0.systems.kafka-atc-repartitioned-requests.streams.CommunicationRequest.partitions.0", "1111")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.ArticleRead.partitions.1", "2222")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.CommunicationRequest.partitions.1", "44444")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.StateChange.partitions.1", "5555")

    val ccl = new CheckpointToolCommandLine
    val result = ccl.parseOffsets(new MapConfig(config))

    assert(result(new TaskName("Partition 0")).size == 2)
    assert(result(new TaskName("Partition 1")).size == 3)
  }
}
