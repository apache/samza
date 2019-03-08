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
import java.util.concurrent.atomic.AtomicReference

import org.apache.samza.config.{ClusterManagerConfig, Config, MapConfig}
import org.apache.samza.context.{ApplicationContainerContext, ContainerContext, JobContext}
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.server.{HttpServer, JobServlet}
import org.apache.samza.job.model.{ContainerModel, JobModel, TaskModel}
import org.apache.samza.metrics.{Gauge, MetricsReporter, Timer}
import org.apache.samza.storage.{ContainerStorageManager, TaskStorageManager}
import org.apache.samza.system._
import org.apache.samza.task.{StreamTaskFactory, TaskFactory}
import org.apache.samza.{Partition, SamzaContainerStatus}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Matchers.{any, notNull}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentCaptor, Mock, Mockito, MockitoAnnotations}
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class TestSamzaContainer extends AssertionsForJUnit with MockitoSugar {
  private val TASK_NAME = new TaskName("taskName")

  @Mock
  private var config: Config = null
  @Mock
  private var taskInstance: TaskInstance = null
  @Mock
  private var runLoop: Runnable = null
  @Mock
  private var systemAdmins: SystemAdmins = null
  @Mock
  private var consumerMultiplexer: SystemConsumers = null
  @Mock
  private var producerMultiplexer: SystemProducers = null
  @Mock
  private var metrics: SamzaContainerMetrics = null
  @Mock
  private var localityManager: LocalityManager = null
  @Mock
  private var containerContext: ContainerContext = null
  @Mock
  private var applicationContainerContext: ApplicationContainerContext = null
  @Mock
  private var samzaContainerListener: SamzaContainerListener = null
  @Mock
  private var containerStorageManager: ContainerStorageManager = null

  private var samzaContainer: SamzaContainer = null

  @Before
  def setup(): Unit = {
    MockitoAnnotations.initMocks(this)
    setupSamzaContainer(Some(this.applicationContainerContext))
    when(this.metrics.containerStartupTime).thenReturn(mock[Timer])
  }

  @Test
  def testExceptionInTaskInitShutsDownTask() {
    when(this.taskInstance.initTask).thenThrow(new RuntimeException("Trigger a shutdown, please."))

    this.samzaContainer.run

    verify(this.taskInstance).shutdownTask
    assertEquals(SamzaContainerStatus.FAILED, this.samzaContainer.getStatus())
    verify(this.samzaContainerListener).beforeStart()
    verify(this.samzaContainerListener, never()).afterStart()
    verify(this.samzaContainerListener, never()).afterStop()
    verify(this.samzaContainerListener).afterFailure(notNull(classOf[Exception]))
    verifyZeroInteractions(this.runLoop)
  }

  @Test
  def testErrorInTaskInitShutsDownTask(): Unit = {
    when(this.taskInstance.initTask).thenThrow(new NoSuchMethodError("Trigger a shutdown, please."))

    this.samzaContainer.run

    verify(this.taskInstance).shutdownTask
    assertEquals(SamzaContainerStatus.FAILED, this.samzaContainer.getStatus())
    verify(this.samzaContainerListener).beforeStart()
    verify(this.samzaContainerListener, never()).afterStart()
    verify(this.samzaContainerListener, never()).afterStop()
    verify(this.samzaContainerListener).afterFailure(notNull(classOf[Exception]))
    verifyZeroInteractions(this.runLoop)
  }

  @Test
  def testExceptionInTaskProcessRunLoop() {
    when(this.runLoop.run()).thenThrow(new RuntimeException("Trigger a shutdown, please."))

    this.samzaContainer.run

    verify(this.taskInstance).shutdownTask
    assertEquals(SamzaContainerStatus.FAILED, this.samzaContainer.getStatus())
    verify(this.samzaContainerListener).beforeStart()
    verify(this.samzaContainerListener).afterStart()
    verify(this.samzaContainerListener, never()).afterStop()
    verify(this.samzaContainerListener).afterFailure(notNull(classOf[Exception]))
    verify(this.runLoop).run()
  }

  @Test
  def testCleanRun(): Unit = {
    doNothing().when(this.runLoop).run() // run loop completes successfully

    this.samzaContainer.run

    verify(this.taskInstance).shutdownTask
    assertEquals(SamzaContainerStatus.STOPPED, this.samzaContainer.getStatus())
    verify(this.samzaContainerListener).beforeStart()
    verify(this.samzaContainerListener).afterStart()
    verify(this.samzaContainerListener).afterStop()
    verify(this.samzaContainerListener, never()).afterFailure(any())
    verify(this.runLoop).run()
  }

  @Test
  def testFailureDuringShutdown(): Unit = {
    doNothing().when(this.runLoop).run() // run loop completes successfully
    when(this.taskInstance.shutdownTask).thenThrow(new RuntimeException("Trigger a shutdown, please."))

    this.samzaContainer.run

    verify(this.taskInstance).shutdownTask
    assertEquals(SamzaContainerStatus.FAILED, this.samzaContainer.getStatus())
    verify(this.samzaContainerListener).beforeStart()
    verify(this.samzaContainerListener).afterStart()
    verify(this.samzaContainerListener, never()).afterStop()
    verify(this.samzaContainerListener).afterFailure(notNull(classOf[Exception]))
    verify(this.runLoop).run()
  }

  @Test
  def testApplicationContainerContext() {
    val orderVerifier = inOrder(this.applicationContainerContext, this.runLoop)
    this.samzaContainer.run
    orderVerifier.verify(this.applicationContainerContext).start()
    orderVerifier.verify(this.runLoop).run()
    orderVerifier.verify(this.applicationContainerContext).stop()
  }

  @Test
  def testNullApplicationContainerContextFactory() {
    setupSamzaContainer(None)
    this.samzaContainer.run
    verify(this.runLoop).run()
    // applicationContainerContext is not even wired into the container anymore, but just double check it is not used
    verifyZeroInteractions(this.applicationContainerContext)
  }

  @Test
  def testReadJobModel() {
    val config = new MapConfig(Map("a" -> "b").asJava)
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system","stream", new Partition(0)), "1")
    val tasks = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(0)))
    val containers = Map(
      "0" -> new ContainerModel("0", tasks),
      "1" -> new ContainerModel("1", tasks))
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
  def testReadJobModelWithTimeouts() {
    val config = new MapConfig(Map("a" -> "b").asJava)
    val offsets = new util.HashMap[SystemStreamPartition, String]()
    offsets.put(new SystemStreamPartition("system","stream", new Partition(0)), "1")
    val tasks = Map(
      new TaskName("t1") -> new TaskModel(new TaskName("t1"), offsets.keySet(), new Partition(0)),
      new TaskName("t2") -> new TaskModel(new TaskName("t2"), offsets.keySet(), new Partition(0)))
    val containers = Map(
      "0" -> new ContainerModel("0", tasks),
      "1" -> new ContainerModel("1", tasks))
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
  def testGetChangelogSSPsForContainer() {
    val taskName0 = new TaskName("task0")
    val taskName1 = new TaskName("task1")
    val taskModel0 = new TaskModel(taskName0,
      Set(new SystemStreamPartition("input", "stream", new Partition(0))),
      new Partition(10))
    val taskModel1 = new TaskModel(taskName1,
      Set(new SystemStreamPartition("input", "stream", new Partition(1))),
      new Partition(11))
    val containerModel = new ContainerModel("processorId", Map(taskName0 -> taskModel0, taskName1 -> taskModel1))
    val changeLogSystemStreams = Map("store0" -> new SystemStream("changelogSystem0", "store0-changelog"),
      "store1" -> new SystemStream("changelogSystem1", "store1-changelog"))
    val expected = Set(new SystemStreamPartition("changelogSystem0", "store0-changelog", new Partition(10)),
      new SystemStreamPartition("changelogSystem1", "store1-changelog", new Partition(10)),
      new SystemStreamPartition("changelogSystem0", "store0-changelog", new Partition(11)),
      new SystemStreamPartition("changelogSystem1", "store1-changelog", new Partition(11)))
    assertEquals(expected, SamzaContainer.getChangelogSSPsForContainer(containerModel, changeLogSystemStreams))
  }

  @Test
  def testGetChangelogSSPsForContainerNoChangelogs() {
    val taskName0 = new TaskName("task0")
    val taskName1 = new TaskName("task1")
    val taskModel0 = new TaskModel(taskName0,
      Set(new SystemStreamPartition("input", "stream", new Partition(0))),
      new Partition(10))
    val taskModel1 = new TaskModel(taskName1,
      Set(new SystemStreamPartition("input", "stream", new Partition(1))),
      new Partition(11))
    val containerModel = new ContainerModel("processorId", Map(taskName0 -> taskModel0, taskName1 -> taskModel1))
    assertEquals(Set(), SamzaContainer.getChangelogSSPsForContainer(containerModel, Map()))
  }

  @Test
  def testStoreContainerLocality():Unit = {
    this.config = new MapConfig(Map(ClusterManagerConfig.JOB_HOST_AFFINITY_ENABLED -> "true"))
    setupSamzaContainer(None) // re-init with an actual config
    val containerModel: ContainerModel = Mockito.mock[ContainerModel](classOf[ContainerModel])
    val testContainerId = "1"
    Mockito.when(containerModel.getId).thenReturn(testContainerId)
    Mockito.when(this.containerContext.getContainerModel).thenReturn(containerModel)

    this.samzaContainer.storeContainerLocality
    Mockito.verify(this.localityManager).writeContainerToHostMapping(any(), any())
  }

  private def setupSamzaContainer(applicationContainerContext: Option[ApplicationContainerContext]) {
    this.samzaContainer = new SamzaContainer(
      this.config,
      Map(TASK_NAME -> this.taskInstance),
      Map(TASK_NAME -> new TaskInstanceMetrics),
      this.runLoop,
      this.systemAdmins,
      this.consumerMultiplexer,
      this.producerMultiplexer,
      this.metrics,
      localityManager = this.localityManager,
      containerContext = this.containerContext,
      applicationContainerContextOption = applicationContainerContext,
      externalContextOption = None,
      containerStorageManager = containerStorageManager)
    this.samzaContainer.setContainerListener(this.samzaContainerListener)
  }

  class MockJobServlet(exceptionLimit: Int, jobModelRef: AtomicReference[JobModel]) extends JobServlet(jobModelRef) {
    var exceptionCount = 0

    override protected def getObjectToWrite(): JobModel = {
      if (exceptionCount < exceptionLimit) {
        exceptionCount += 1
        throw new java.io.IOException("Throwing exception")
      } else {
        val jobModel = jobModelRef.get()
        jobModel
      }
    }
  }
}
