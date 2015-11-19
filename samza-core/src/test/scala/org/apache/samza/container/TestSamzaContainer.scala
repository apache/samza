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

package org.apache.samza.container

import java.util
import org.apache.samza.storage.TaskStorageManager
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.job.model.ContainerModel
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.task.ClosableTask
import org.apache.samza.task.InitableTask
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskContext
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit
import java.lang.Thread.UncaughtExceptionHandler
import org.apache.samza.serializers._
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.mockito.Mockito._

class TestSamzaContainer extends AssertionsForJUnit with MockitoSugar {
  @Test
  def testReadJobModel {
    val config = new MapConfig(Map("a" -> "b"))
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system","stream", new Partition(0)), "1")
    val tasks = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(0)))
    val containers = Map(
      Integer.valueOf(0) -> new ContainerModel(0, tasks),
      Integer.valueOf(1) -> new ContainerModel(1, tasks))
    val jobModel = new JobModel(config, containers)
    def jobModelGenerator(): JobModel = jobModel
    val server = new HttpServer
    val coordinator = new JobCoordinator(jobModel, server)
    coordinator.server.addServlet("/*", new JobServlet(jobModelGenerator))
    try {
      coordinator.start
      assertEquals(jobModel, SamzaContainer.readJobModel(server.getUrl.toString))
    } finally {
      coordinator.stop
    }
  }

  @Test
  def testChangelogPartitions {
    val config = new MapConfig(Map("a" -> "b"))
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system", "stream", new Partition(0)), "1")
    val tasksForContainer1 = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(1)))
    val tasksForContainer2 = Map(
      new TaskName("t3") -> new TaskModel(new TaskName("t3"), offsets.keySet(), new Partition(2)),
      new TaskName("t4") -> new TaskModel(new TaskName("t4"), offsets.keySet(), new Partition(3)),
      new TaskName("t5") -> new TaskModel(new TaskName("t6"), offsets.keySet(), new Partition(4)))
    val containerModel1 = new ContainerModel(0, tasksForContainer1)
    val containerModel2 = new ContainerModel(1, tasksForContainer2)
    val containers = Map(
      Integer.valueOf(0) -> containerModel1,
      Integer.valueOf(1) -> containerModel2)
    val jobModel = new JobModel(config, containers)
    assertEquals(jobModel.maxChangeLogStreamPartitions, 5)
  }

  @Test
  def testGetInputStreamMetadata {
    val inputStreams = Set(
      new SystemStreamPartition("test", "stream1", new Partition(0)),
      new SystemStreamPartition("test", "stream1", new Partition(1)),
      new SystemStreamPartition("test", "stream2", new Partition(0)),
      new SystemStreamPartition("test", "stream2", new Partition(1)))
    val systemAdmins = Map("test" -> new SinglePartitionWithoutOffsetsSystemAdmin)
    val metadata = new StreamMetadataCache(systemAdmins).getStreamMetadata(inputStreams.map(_.getSystemStream).toSet)
    assertNotNull(metadata)
    assertEquals(2, metadata.size)
    val stream1Metadata = metadata(new SystemStream("test", "stream1"))
    val stream2Metadata = metadata(new SystemStream("test", "stream2"))
    assertNotNull(stream1Metadata)
    assertNotNull(stream2Metadata)
    assertEquals("stream1", stream1Metadata.getStreamName)
    assertEquals("stream2", stream2Metadata.getStreamName)
  }

  @Test
  def testExceptionInTaskInitShutsDownTask {
    val task = new StreamTask with InitableTask with ClosableTask {
      var wasShutdown = false

      def init(config: Config, context: TaskContext) {
        throw new Exception("Trigger a shutdown, please.")
      }

      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
      }

      def close {
        wasShutdown = true
      }
    }
    val config = new MapConfig
    val taskName = new TaskName("taskName")
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      new TaskInstanceMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext
    )
    val runLoop = new RunLoop(
      taskInstances = Map(taskName -> taskInstance),
      consumerMultiplexer = consumerMultiplexer,
      metrics = new SamzaContainerMetrics)
    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = runLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null
    )
    try {
      container.run
      fail("Expected exception to be thrown in run method.")
    } catch {
      case e: Exception => // Expected
    }
    assertTrue(task.wasShutdown)
  }

  @Test
  def testUncaughtExceptionHandler {
    var caughtException = false
    val exceptionHandler = new UncaughtExceptionHandler {
      def uncaughtException(t: Thread, e: Throwable) {
        caughtException = true
      }
    }
    try {
      SamzaContainer.safeMain(() => null, exceptionHandler)
    } catch {
      case _: Exception =>
      // Expect some random exception from SamzaContainer because we haven't
      // set any environment variables for container ID, etc.
    }
    assertFalse(caughtException)
    val t = new Thread(new Runnable {
      def run = throw new RuntimeException("Uncaught exception in another thread. Catch this.")
    })
    t.start
    t.join
    assertTrue(caughtException)
  }

  @Test
  def testStartStoresIncrementsCounter {
    val task = new StreamTask {
      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
      }
    }
    val config = new MapConfig
    val taskName = new TaskName("taskName")
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))
    val mockTaskStorageManager = mock[TaskStorageManager]

    when(mockTaskStorageManager.init).thenAnswer(new Answer[String] {
      override def answer(invocation: InvocationOnMock): String = {
        Thread.sleep(1)
        ""
      }
    })

    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      new TaskInstanceMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext,
      storageManager = mockTaskStorageManager
    )
    val containerMetrics = new SamzaContainerMetrics()
    containerMetrics.addStoreRestorationGauge(taskName, "store")
    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = null,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = containerMetrics,
      jmxServer = null
    )
    container.startStores
    assertNotNull(containerMetrics.taskStoreRestorationMetrics)
    assertNotNull(containerMetrics.taskStoreRestorationMetrics.get(taskName))
    assertTrue(containerMetrics.taskStoreRestorationMetrics.get(taskName).getValue >= 1)

  }
}

class MockCheckpointManager extends CheckpointManager {
  override def start() = {}
  override def stop() = {}

  override def register(taskName: TaskName): Unit = {}

  override def readLastCheckpoint(taskName: TaskName): Checkpoint = { new Checkpoint(Map[SystemStreamPartition, String]()) }

  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint): Unit = { }
}
