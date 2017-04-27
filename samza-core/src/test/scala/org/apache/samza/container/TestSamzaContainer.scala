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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import org.apache.samza.{SamzaContainerStatus, Partition}
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.server.{HttpServer, JobServlet}
import org.apache.samza.job.model.{ContainerModel, JobModel, TaskModel}
import org.apache.samza.processor.SamzaContainerListener
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.storage.TaskStorageManager
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.system.{IncomingMessageEnvelope, StreamMetadataCache, SystemConsumer, SystemConsumers, SystemProducer, SystemProducers, SystemStream, SystemStreamPartition}
import org.apache.samza.task.{ClosableTask, InitableTask, MessageCollector, StreamTask, TaskContext, TaskCoordinator, TaskInstanceCollector}
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.junit.Assert._
import org.junit.Test
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._
import org.mockito.Mockito.when
import scala.collection.JavaConversions._

class TestSamzaContainer extends AssertionsForJUnit with MockitoSugar {
  @Test
  def testReadJobModel {
    val config = new MapConfig(Map("a" -> "b").asJava)
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system","stream", new Partition(0)), "1")
    val tasks = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(0)))
    val containers = Map(
      "0" -> new ContainerModel("0", 0, tasks),
      "1" -> new ContainerModel("1", 0, tasks))
    val jobModel = new JobModel(config, containers)
    def jobModelGenerator(): JobModel = jobModel
    val server = new HttpServer
    val coordinator = new JobModelManager(jobModel, server)
    JobModelManager.jobModelRef.set(jobModelGenerator())
    coordinator.server.addServlet("/*", new JobServlet(JobModelManager.jobModelRef))
    try {
      coordinator.start
      assertEquals(jobModel, SamzaContainer.readJobModel(server.getUrl.toString))
    } finally {
      coordinator.stop
    }
  }

  @Test
  def testReadJobModelWithTimeouts {
    val config = new MapConfig(Map("a" -> "b").asJava)
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system","stream", new Partition(0)), "1")
    val tasks = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(0)))
    val containers = Map(
      "0" -> new ContainerModel("0", 0, tasks),
      "1" -> new ContainerModel("1", 1, tasks))
    val jobModel = new JobModel(config, containers)
    def jobModelGenerator(): JobModel = jobModel
    val server = new HttpServer
    val coordinator = new JobModelManager(jobModel, server)
    JobModelManager.jobModelRef.set(jobModelGenerator())
    val mockJobServlet = new MockJobServlet(2, JobModelManager.jobModelRef)
    coordinator.server.addServlet("/*", mockJobServlet)
    try {
      coordinator.start
      assertEquals(jobModel, SamzaContainer.readJobModel(server.getUrl.toString))
    } finally {
      coordinator.stop
    }
    assertEquals(2, mockJobServlet.exceptionCount)
  }

  @Test
  def testChangelogPartitions {
    val config = new MapConfig(Map("a" -> "b").asJava)
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system", "stream", new Partition(0)), "1")
    val tasksForContainer1 = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(1)))
    val tasksForContainer2 = Map(
      new TaskName("t3") -> new TaskModel(new TaskName("t3"), offsets.keySet(), new Partition(2)),
      new TaskName("t4") -> new TaskModel(new TaskName("t4"), offsets.keySet(), new Partition(3)),
      new TaskName("t5") -> new TaskModel(new TaskName("t6"), offsets.keySet(), new Partition(4)))
    val containerModel1 = new ContainerModel("0", 0, tasksForContainer1)
    val containerModel2 = new ContainerModel("1", 1, tasksForContainer2)
    val containers = Map(
      "0" -> containerModel1,
      "1" -> containerModel2)
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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
      metrics = new SamzaContainerMetrics,
      maxThrottlingDelayMs = TimeUnit.SECONDS.toMillis(1))
    @volatile var onContainerFailedCalled = false
    @volatile var onContainerStopCalled = false
    @volatile var onContainerStartCalled = false
    @volatile var onContainerFailedThrowable: Throwable = null

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = runLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null,
      containerListener = new SamzaContainerListener {
        override def onContainerFailed(t: Throwable): Unit = {
          onContainerFailedCalled = true
          onContainerFailedThrowable = t
        }

        override def onContainerStop(invokedExternally: Boolean): Unit = {
          onContainerStopCalled = true
        }

        override def onContainerStart(): Unit = {
          onContainerStartCalled = true
        }
      }
    )
    container.run
    assertTrue(task.wasShutdown)
    assertFalse(onContainerStartCalled)
    assertFalse(onContainerStopCalled)

    assertTrue(onContainerFailedCalled)
    assertNotNull(onContainerFailedThrowable)
  }

  // Exception in Runloop should cause SamzaContainer to transition to FAILED status, shutdown the components and then,
  // invoke the callback
  @Test
  def testExceptionInTaskProcessRunLoop() {
    val task = new StreamTask with InitableTask with ClosableTask {
      var wasShutdown = false

      def init(config: Config, context: TaskContext) {
      }

      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
        throw new Exception("Trigger a shutdown, please.")
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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

    @volatile var onContainerFailedCalled = false
    @volatile var onContainerStopCalled = false
    @volatile var onContainerStartCalled = false
    @volatile var onContainerFailedThrowable: Throwable = null

    val mockRunLoop = mock[RunLoop]
    when(mockRunLoop.run).thenThrow(new RuntimeException("Trigger a shutdown, please."))

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = mockRunLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null,
      containerListener = new SamzaContainerListener {
        override def onContainerFailed(t: Throwable): Unit = {
          onContainerFailedCalled = true
          onContainerFailedThrowable = t
        }

        override def onContainerStop(invokedExternally: Boolean): Unit = {
          onContainerStopCalled = true
        }

        override def onContainerStart(): Unit = {
          onContainerStartCalled = true
        }
      }
    )
    container.run
    assertTrue(task.wasShutdown)
    assertTrue(onContainerStartCalled)

    assertFalse(onContainerStopCalled)

    assertTrue(onContainerFailedCalled)
    assertNotNull(onContainerFailedThrowable)

    assertEquals(SamzaContainerStatus.FAILED, container.getStatus())
  }

  @Test
  def testErrorInTaskInitShutsDownTask() {
    val task = new StreamTask with InitableTask with ClosableTask {
      var wasShutdown = false

      def init(config: Config, context: TaskContext) {
        throw new NoSuchMethodError("Trigger a shutdown, please.")
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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
      metrics = new SamzaContainerMetrics,
      maxThrottlingDelayMs = TimeUnit.SECONDS.toMillis(1))
    @volatile var onContainerFailedCalled = false
    @volatile var onContainerStopCalled = false
    @volatile var onContainerStartCalled = false
    @volatile var onContainerFailedThrowable: Throwable = null

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = runLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null,
      containerListener = new SamzaContainerListener {
        override def onContainerFailed(t: Throwable): Unit = {
          onContainerFailedCalled = true
          onContainerFailedThrowable = t
        }

        override def onContainerStop(invokedExternally: Boolean): Unit = {
          onContainerStopCalled = true
        }

        override def onContainerStart(): Unit = {
          onContainerStartCalled = true
        }
      }
    )
    container.run

    assertTrue(task.wasShutdown)

    assertFalse(onContainerStopCalled)
    assertFalse(onContainerStartCalled)

    assertTrue(onContainerFailedCalled)
    assertNotNull(onContainerFailedThrowable)
  }

  @Test
  def testRunloopShutdownIsClean(): Unit = {
    val task = new StreamTask with InitableTask with ClosableTask {
      var wasShutdown = false

      def init(config: Config, context: TaskContext) {
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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

    @volatile var onContainerFailedCalled = false
    @volatile var onContainerStopCalled = false
    @volatile var onContainerStartCalled = false
    @volatile var onContainerFailedThrowable: Throwable = null

    val mockRunLoop = mock[RunLoop]
    when(mockRunLoop.run).then(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        Thread.sleep(100)
      }
    })

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = mockRunLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null,
      containerListener = new SamzaContainerListener {
        override def onContainerFailed(t: Throwable): Unit = {
          onContainerFailedCalled = true
          onContainerFailedThrowable = t
        }

        override def onContainerStop(invokedExternally: Boolean): Unit = {
          onContainerStopCalled = true
        }

        override def onContainerStart(): Unit = {
          onContainerStartCalled = true
        }
      }
    )
    container.run
    assertFalse(onContainerFailedCalled)
    assertTrue(onContainerStartCalled)
    assertTrue(onContainerStopCalled)
  }

  @Test
  def testFailureDuringShutdown: Unit = {
    val task = new StreamTask with InitableTask with ClosableTask {
      def init(config: Config, context: TaskContext) {
      }

      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {

      }

      def close {
        throw new Exception("Exception during shutdown, please.")
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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

    @volatile var onContainerFailedCalled = false
    @volatile var onContainerStopCalled = false
    @volatile var onContainerStartCalled = false
    @volatile var onContainerFailedThrowable: Throwable = null

    val mockRunLoop = mock[RunLoop]
    when(mockRunLoop.run).then(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        Thread.sleep(100)
      }
    })

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = mockRunLoop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null,
      containerListener = new SamzaContainerListener {
        override def onContainerFailed(t: Throwable): Unit = {
          onContainerFailedCalled = true
          onContainerFailedThrowable = t
        }

        override def onContainerStop(invokedExternally: Boolean): Unit = {
          onContainerStopCalled = true
        }

        override def onContainerStart(): Unit = {
          onContainerStartCalled = true
        }
      }
    )
    container.run

    assertTrue(onContainerFailedCalled)
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
    val containerContext = new SamzaContainerContext("0", config, Set[TaskName](taskName))
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
      jmxServer = null,
      containerListener = new SamzaContainerListener {override def onContainerFailed(t: Throwable): Unit = { }

        override def onContainerStop(invokedExternally: Boolean): Unit = { }

        override def onContainerStart(): Unit = { }
      }
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

  override def readLastCheckpoint(taskName: TaskName): Checkpoint = { new Checkpoint(Map[SystemStreamPartition, String]().asJava) }

  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint): Unit = { }
}

class MockJobServlet(exceptionLimit: Int, jobModelRef: AtomicReference[JobModel]) extends JobServlet(jobModelRef) {
  var exceptionCount = 0

  override protected def getObjectToWrite() = {
    if (exceptionCount < exceptionLimit) {
      exceptionCount += 1
      throw new java.io.IOException("Throwing exception")
    } else {
      val jobModel = jobModelRef.get()
      jobModel
    }
  }
}
