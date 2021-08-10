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

import java.util.{Collections, Properties}
import org.apache.samza.Partition
import org.apache.samza.checkpoint.CheckpointTool.{CheckpointToolCommandLine, TaskNameToCheckpointMap}
import org.apache.samza.checkpoint.TestCheckpointTool.{MockCheckpointManagerFactory, MockSystemFactory}
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.metadatastore.{CoordinatorStreamStore, CoordinatorStreamStoreTestUtil}
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory
import org.apache.samza.execution.JobPlanner
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.{Before, Test}
import org.mockito.Matchers._
import org.mockito.{ArgumentCaptor, Matchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

object TestCheckpointTool {
  var checkpointManager: CheckpointManager = _
  var systemConsumer: SystemConsumer = _
  var systemProducer: SystemProducer = _
  var systemAdmin: SystemAdmin = _
  var coordinatorConfig: Config = new MapConfig()

  class MockCheckpointManagerFactory extends CheckpointManagerFactory {
    def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = {
      coordinatorConfig = config
      checkpointManager
    }
  }

  class MockSystemFactory extends SystemFactory {
    override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = systemConsumer
    override def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = systemProducer
    override def getAdmin(systemName: String, config: Config) = systemAdmin
  }
}

class TestCheckpointTool extends AssertionsForJUnit with MockitoSugar {
  var config: MapConfig = _

  val tn0 = new TaskName("Partition 0")
  val tn1 = new TaskName("Partition 1")
  val tn2 = new TaskName("Partition 2")
  val p0 = new Partition(0)
  val p1 = new Partition(1)
  val p2 = new Partition(2)

  @Before
  def setup() {
    val userDefinedConfig: MapConfig = new MapConfig(Map(
      ApplicationConfig.APP_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      TaskConfig.INPUT_STREAMS -> "test.foo",
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getName,
      SystemConfig.SYSTEM_FACTORY_FORMAT.format("test") -> classOf[MockSystemFactory].getName,
      SystemConfig.SYSTEM_FACTORY_FORMAT.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName,
      TaskConfig.GROUPER_FACTORY -> "org.apache.samza.container.grouper.task.GroupByContainerCountFactory"
    ).asJava)
    config = JobPlanner.generateSingleJobConfig(userDefinedConfig)
    val metadata = new SystemStreamMetadata("foo", Map[Partition, SystemStreamPartitionMetadata](
      p0 -> new SystemStreamPartitionMetadata("0", "100", "101"),
      p1 -> new SystemStreamPartitionMetadata("0", "200", "201"),
      p2 -> new SystemStreamPartitionMetadata("0", "300", "301")
    ).asJava)
    TestCheckpointTool.checkpointManager = mock[CheckpointManager]
    TestCheckpointTool.systemAdmin = mock[SystemAdmin]
    when(TestCheckpointTool.systemAdmin.getSystemStreamPartitionCounts(Set("foo").asJava, 0))
      .thenReturn(Map("foo" -> metadata).asJava)
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn0))
      .thenReturn(new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p0) -> "1234").asJava))
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn1))
      .thenReturn(new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p1) -> "4321").asJava))
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn2))
      .thenReturn(new CheckpointV2(null,
        Map(new SystemStreamPartition("test", "foo", p2) -> "5678").asJava,
        Map("BackupFactory"-> Map("StoreName"-> "offset").asJava).asJava
      ))
  }

  @Test
  def testReadLatestCheckpoint() {
    val checkpointTool = CheckpointTool(config, null)
    checkpointTool.run()
    verify(TestCheckpointTool.checkpointManager).readLastCheckpoint(tn0)
    verify(TestCheckpointTool.checkpointManager).readLastCheckpoint(tn1)
    verify(TestCheckpointTool.checkpointManager, never()).writeCheckpoint(any(), any())
  }

  @Test
  def testOverwriteCheckpoint() {
    val toOverwrite = Map(tn0 -> Map(new SystemStreamPartition("test", "foo", p0) -> "42"),
      tn1 -> Map(new SystemStreamPartition("test", "foo", p1) -> "43"))

    val checkpointTool = CheckpointTool(config, toOverwrite)
    checkpointTool.run()
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn0, new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p0) -> "42").asJava))
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn1, new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p1) -> "43").asJava))
  }

  @Test
  def testOverwriteCheckpointV2() {
    // Skips the v1 checkpoints for the task
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn0))
      .thenReturn(null)
    when(TestCheckpointTool.checkpointManager.readLastCheckpoint(tn1))
      .thenReturn(null)

    val toOverwrite = Map(
      tn0 -> Map(new SystemStreamPartition("test", "foo", p0) -> "42"),
      tn1 -> Map(new SystemStreamPartition("test", "foo", p1) -> "43"),
      tn2 -> Map(new SystemStreamPartition("test", "foo", p2) -> "45"))

    val checkpointV2Config = new MapConfig(config, Map(TaskConfig.CHECKPOINT_READ_VERSIONS -> "2").asJava)

    val argument = ArgumentCaptor.forClass(classOf[CheckpointV2])
    val checkpointTool = CheckpointTool(checkpointV2Config, toOverwrite)

    checkpointTool.run()
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(Matchers.same(tn0), argument.capture())
    assertNotNull(argument.getValue.getCheckpointId)
    assertEquals(Map(new SystemStreamPartition("test", "foo", p0) -> "42").asJava, argument.getValue.getOffsets)
    assertEquals(Collections.emptyMap(), argument.getValue.getStateCheckpointMarkers)
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(Matchers.same(tn1), argument.capture())
    assertNotNull(argument.getValue.getCheckpointId)
    assertEquals(Map(new SystemStreamPartition("test", "foo", p1) -> "43").asJava, argument.getValue.getOffsets)
    assertEquals(Collections.emptyMap(), argument.getValue.getStateCheckpointMarkers)
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(Matchers.same(tn2), argument.capture())
    assertNotNull(argument.getValue.getCheckpointId)
    assertEquals(Map(new SystemStreamPartition("test", "foo", p2) -> "45").asJava, argument.getValue.getOffsets)
    assertEquals(Map("BackupFactory"-> Map("StoreName"-> "offset").asJava).asJava, argument.getValue.getStateCheckpointMarkers)
  }

  @Test
  def testGrouping(): Unit = {
    val config : java.util.Properties = new Properties()
    config.put("tasknames.Partition 0.systems.kafka-atc-repartitioned-requests.streams.ArticleRead.partitions.0", "0000")
    config.put("tasknames.Partition 0.systems.kafka-atc-repartitioned-requests.streams.CommunicationRequest.partitions.0", "1111")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.ArticleRead.partitions.1", "2222")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.CommunicationRequest.partitions.1", "44444")
    config.put("tasknames.Partition 1.systems.kafka-atc-repartitioned-requests.streams.StateChange.partitions.1", "5555")

    val ccl = new CheckpointToolCommandLine
    val result = ccl.parseOffsets(config)

    assert(result(new TaskName("Partition 0")).size == 2)
    assert(result(new TaskName("Partition 1")).size == 3)
  }

  @Test
  def testShouldInstantiateCheckpointManagerWithConfigurationFromCoordinatorStream(): Unit = {
    val offsetMap: TaskNameToCheckpointMap = Map(tn0 -> Map(new SystemStreamPartition("test", "foo", p0) -> "42"),
      tn1 -> Map(new SystemStreamPartition("test", "foo", p1) -> "43"))
    val userDefinedConfig: MapConfig = new MapConfig(Map(
      ApplicationConfig.APP_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      TaskConfig.INPUT_STREAMS -> "test.foo",
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getName,
      SystemConfig.SYSTEM_FACTORY_FORMAT.format("test") -> classOf[MockSystemFactory].getName,
      SystemConfig.SYSTEM_FACTORY_FORMAT.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName,
      TaskConfig.GROUPER_FACTORY -> "org.apache.samza.container.grouper.task.GroupByContainerCountFactory"
    ).asJava)
    val generatedConfigs: MapConfig = JobPlanner.generateSingleJobConfig(userDefinedConfig)
    val coordinatorStreamStoreTestUtil: CoordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(config)


    val coordinatorStreamStore: CoordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore
    val checkpointTool: CheckpointTool = Mockito.spy(new CheckpointTool(offsetMap, coordinatorStreamStore, generatedConfigs))
    Mockito.when(checkpointTool.getConfigFromCoordinatorStream(coordinatorStreamStore)).thenReturn(userDefinedConfig)
    checkpointTool.run()

    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn0, new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p0) -> "42").asJava))
    verify(TestCheckpointTool.checkpointManager)
      .writeCheckpoint(tn1, new CheckpointV1(Map(new SystemStreamPartition("test", "foo", p1) -> "43").asJava))

    // Two configurations job.id, job.name are populated in the coordinator config by SamzaRuntime and it is not present in generated config.
    assert(generatedConfigs.entrySet().containsAll(TestCheckpointTool.coordinatorConfig.entrySet()))
  }
}
