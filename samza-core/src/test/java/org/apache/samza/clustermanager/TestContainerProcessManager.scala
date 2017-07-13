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
 *//*
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
package org.apache.samza.clustermanager

import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig
import org.apache.samza.config.MapConfig
import org.apache.samza.container.LocalityManager
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.JobModelManagerTestUtil
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.testUtils.MockHttpServer
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.lang.reflect.Field
import java.util
import java.util.concurrent.CountDownLatch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

object TestContainerProcessManager {
  private var isRunning = false
}

class TestContainerProcessManager {
  final private val callback = new MockClusterResourceManagerCallback
  final private val manager = new MockClusterResourceManager(callback)
  private val configVals = new util.HashMap[String, String]() {}
  private val config = new MapConfig(configVals)

  private def getConfig = {
    val map = new util.HashMap[String, String]
    map.putAll(config)
    new MapConfig(map)
  }

  private def getConfigWithHostAffinity = {
    val map = new util.HashMap[String, String]
    map.putAll(config)
    map.put("job.host-affinity.enabled", "true")
    new MapConfig(map)
  }

  private var server = null
  private var state = null

  private def getJobModelManagerWithHostAffinity(containerCount: Int) = {
    val localityMap = new util.HashMap[String, util.Map[String, String]]
    localityMap.put("0", new util.HashMap[String, String]() {})
    val mockLocalityManager = mock(classOf[LocalityManager])
    when(mockLocalityManager.readContainerLocality).thenReturn(localityMap)
    JobModelManagerTestUtil.getJobModelManagerWithLocalityManager(getConfig, containerCount, mockLocalityManager, this.server)
  }

  private def getJobModelManagerWithoutHostAffinity(containerCount: Int) = JobModelManagerTestUtil.getJobModelManager(getConfig, containerCount, this.server)

  @Before
  @throws[Exception]
  def setup(): Unit = server = new MockHttpServer("/", 7777, null, new ServletHolder(classOf[DefaultServlet]))

  @throws[Exception]
  private def getPrivateFieldFromTaskManager(fieldName: String, `object`: ContainerProcessManager) = {
    val field = `object`.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  @Test
  @throws[Exception]
  def testContainerProcessManager(): Unit = {
    val conf = new util.HashMap[String, String]
    conf.putAll(getConfig)
    conf.put("yarn.container.memory.mb", "500")
    conf.put("yarn.container.cpu.cores", "5")
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    var taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    var allocator = getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager).asInstanceOf[AbstractContainerAllocator]
    assertEquals(classOf[ContainerAllocator], allocator.getClass)
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMemoryMb)
    assertEquals(5, allocator.containerNumCpuCores)
    conf.clear()
    conf.putAll(getConfigWithHostAffinity)
    conf.put("yarn.container.memory.mb", "500")
    conf.put("yarn.container.cpu.cores", "5")
    state = new SamzaApplicationState(getJobModelManagerWithHostAffinity(1))
    taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    allocator = getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager).asInstanceOf[AbstractContainerAllocator]
    assertEquals(classOf[HostAwareContainerAllocator], allocator.getClass)
    assertEquals(500, allocator.containerMemoryMb)
    assertEquals(5, allocator.containerNumCpuCores)
  }

  @Test
  @throws[Exception]
  def testOnInit(): Unit = {
    val conf = getConfig
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state, 1)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val latch = new CountDownLatch(1)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, new Thread() {
      override def run(): Unit = {
        TestContainerProcessManager.isRunning = true
        latch.countDown()
      }
    })
    taskManager.start()
    latch.await()
    // Verify Allocator thread has started running
    assertTrue(TestContainerProcessManager.isRunning)
    // Verify the remaining state
    assertEquals(1, state.neededContainers.get)
    assertEquals(1, allocator.requestedContainers)
    taskManager.stop()
  }

  @Test
  @throws[Exception]
  def testOnShutdown(): Unit = {
    val conf = getConfig
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    taskManager.start()
    val allocatorThread = getPrivateFieldFromTaskManager("allocatorThread", taskManager).get(taskManager).asInstanceOf[Thread]
    assertTrue(allocatorThread.isAlive)
    taskManager.stop()
    assertFalse(allocatorThread.isAlive)
  }

  /**
    * Test Task Manager should stop when all containers finish
    */
  @Test
  @throws[Exception]
  def testTaskManagerShouldStopWhenContainersFinish(): Unit = {
    val conf = getConfig
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val thread = new Thread(allocator)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread)
    // start triggers a request
    taskManager.start()
    assertFalse(taskManager.shouldShutdown)
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    val container = new SamzaResource(1, 1024, "abc", "id0")
    taskManager.onResourceAllocated(container)
    // Allow container to run and update state
    allocator.awaitContainersStart(1)
    assertFalse(taskManager.shouldShutdown)
    taskManager.onResourceCompleted(new SamzaResourceStatus("id0", "diagnostics", SamzaResourceStatus.SUCCESS))
    assertTrue(taskManager.shouldShutdown)
  }

  /**
    * Test Task Manager should request a new container when a task fails with unknown exit code
    * When host-affinity is not enabled, it will always request for ANY_HOST
    */
  @Test
  @throws[Exception]
  def testNewContainerRequestedOnFailureWithUnknownCode(): Unit = {
    val conf = getConfig
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val thread = new Thread(allocator)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread)
    taskManager.start()
    assertFalse(taskManager.shouldShutdown)
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    val container = new SamzaResource(1, 1024, "abc", "id0")
    taskManager.onResourceAllocated(container)
    allocator.awaitContainersStart(1)
    // Create first container failure
    taskManager.onResourceCompleted(new SamzaResourceStatus(container.getResourceID, "diagnostics", 1))
    // The above failure should trigger a container request
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState.peekPendingRequest.getPreferredHost)
    assertFalse(taskManager.shouldShutdown)
    assertFalse(state.jobHealthy.get)
    assertEquals(2, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    taskManager.onResourceAllocated(container)
    allocator.awaitContainersStart(1)
    assertTrue(state.jobHealthy.get)
    // Create a second failure
    taskManager.onResourceCompleted(new SamzaResourceStatus(container.getResourceID, "diagnostics", 1))
    // The above failure should trigger a job shutdown because our retry count is set to 1
    assertEquals(0, allocator.getContainerRequestState.numPendingRequests)
    assertEquals(2, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    assertFalse(state.jobHealthy.get)
    assertTrue(taskManager.shouldShutdown)
    assertEquals(SamzaApplicationState.SamzaAppStatus.FAILED, state.status)
    taskManager.stop()
  }

  @Test
  @throws[Exception]
  def testInvalidNotificationsAreIgnored(): Unit = {
    val conf = getConfig
    val config = new util.HashMap[String, String]
    config.putAll(getConfig)
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val thread = new Thread(allocator)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread)
    // Start the task manager
    taskManager.start()
    val container = new SamzaResource(1, 1000, "abc", "id1")
    taskManager.onResourceAllocated(container)
    allocator.awaitContainersStart(1)
    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    taskManager.onResourceCompleted(new SamzaResourceStatus("invalidContainerID", "Disk failure", SamzaResourceStatus.DISK_FAIL))
    // The above failure should not trigger any container requests, since it is for an invalid container ID
    assertEquals(0, allocator.getContainerRequestState.numPendingRequests)
    assertFalse(taskManager.shouldShutdown)
    assertTrue(state.jobHealthy.get)
    assertEquals(state.invalidNotifications.get, 1)
  }

  @Test
  @throws[Exception]
  def testDuplicateNotificationsDoNotAffectJobHealth(): Unit = {
    val conf = getConfig
    val config = new util.HashMap[String, String]
    config.putAll(getConfig)
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val thread = new Thread(allocator)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread)
    taskManager.start()
    assertFalse(taskManager.shouldShutdown)
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    val container1 = new SamzaResource(1, 1000, "abc", "id1")
    taskManager.onResourceAllocated(container1)
    allocator.awaitContainersStart(1)
    assertEquals(0, allocator.getContainerRequestState.numPendingRequests)
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID, "Disk failure", SamzaResourceStatus.DISK_FAIL))
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    assertFalse(taskManager.shouldShutdown)
    assertFalse(state.jobHealthy.get)
    assertEquals(2, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState.peekPendingRequest.getPreferredHost)
    val container2 = new SamzaResource(1, 1000, "abc", "id2")
    taskManager.onResourceAllocated(container2)
    allocator.awaitContainersStart(1)
    assertTrue(state.jobHealthy.get)
    // Simulate a duplicate notification for container 1 with a different exit code
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID, "Disk failure", SamzaResourceStatus.PREEMPTED))
    // assert that a duplicate notification does not change metrics (including job health)
    assertEquals(state.invalidNotifications.get, 1)
    assertEquals(2, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    assertTrue(state.jobHealthy.get)
  }

  /**
    * Test AM requests a new container when a task fails
    * Error codes with same behavior - Disk failure, preemption and aborted
    */
  @Test
  @throws[Exception]
  def testNewContainerRequestedOnFailureWithKnownCode(): Unit = {
    val conf = getConfig
    val config = new util.HashMap[String, String]
    config.putAll(getConfig)
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    val allocator = new MockContainerAllocator(manager, conf, state)
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator)
    val thread = new Thread(allocator)
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread)
    taskManager.start()
    assertFalse(taskManager.shouldShutdown)
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    val container1 = new SamzaResource(1, 1000, "abc", "id1")
    taskManager.onResourceAllocated(container1)
    allocator.awaitContainersStart(1)
    assertEquals(0, allocator.getContainerRequestState.numPendingRequests)
    taskManager.onResourceCompleted(new SamzaResourceStatus(container1.getResourceID, "Disk failure", SamzaResourceStatus.DISK_FAIL))
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    assertFalse(taskManager.shouldShutdown)
    assertFalse(state.jobHealthy.get)
    assertEquals(2, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState.peekPendingRequest.getPreferredHost)
    val container2 = new SamzaResource(1, 1000, "abc", "id2")
    taskManager.onResourceAllocated(container2)
    allocator.awaitContainersStart(1)
    // Create container failure - with ContainerExitStatus.PREEMPTED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container2.getResourceID, "Preemption", SamzaResourceStatus.PREEMPTED))
    assertEquals(3, manager.resourceRequests.size)
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    assertFalse(taskManager.shouldShutdown)
    assertFalse(state.jobHealthy.get)
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState.peekPendingRequest.getPreferredHost)
    val container3 = new SamzaResource(1, 1000, "abc", "id3")
    taskManager.onResourceAllocated(container3)
    allocator.awaitContainersStart(1)
    // Create container failure - with ContainerExitStatus.ABORTED
    taskManager.onResourceCompleted(new SamzaResourceStatus(container3.getResourceID, "Aborted", SamzaResourceStatus.ABORTED))
    assertEquals(1, allocator.getContainerRequestState.numPendingRequests)
    assertEquals(4, manager.resourceRequests.size)
    assertEquals(0, manager.releasedResources.size)
    assertFalse(taskManager.shouldShutdown)
    assertFalse(state.jobHealthy.get)
    assertEquals(ResourceRequestState.ANY_HOST, allocator.getContainerRequestState.peekPendingRequest.getPreferredHost)
    taskManager.stop()
  }

  @Test def testAppMasterWithFwk(): Unit = {
    val conf = getConfig
    state = new SamzaApplicationState(getJobModelManagerWithoutHostAffinity(1))
    val taskManager = new ContainerProcessManager(new MapConfig(conf), state, new MetricsRegistryMap, manager)
    taskManager.start()
    val container2 = new SamzaResource(1, 1024, "", "id0")
    assertFalse(taskManager.shouldShutdown)
    taskManager.onResourceAllocated(container2)
    configVals.put(JobConfig.SAMZA_FWK_PATH, "/export/content/whatever")
    val config1 = new MapConfig(configVals)
    val taskManager1 = new ContainerProcessManager(new MapConfig(config), state, new MetricsRegistryMap, manager)
    taskManager1.start()
    taskManager1.onResourceAllocated(container2)
  }

  @After def teardown(): Unit = server.stop()
}