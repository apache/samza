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

package org.apache.samza.job.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.job.yarn.util.MockContainerAllocator;
import org.apache.samza.job.yarn.util.MockHttpServer;
import org.apache.samza.job.yarn.util.TestAMRMClientImpl;
import org.apache.samza.job.yarn.util.TestUtil;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSamzaTaskManager {
  private AMRMClientAsyncImpl amRmClientAsync;
  private TestAMRMClientImpl testAMRMClient;

  private static volatile boolean isRunning = false;

  private Config config = new MapConfig(new HashMap<String, String>() {
    {
      put("yarn.container.count", "1");
      put("systems.test-system.samza.factory", "org.apache.samza.job.yarn.MockSystemFactory");
      put("yarn.container.memory.mb", "512");
      put("yarn.package.path", "/foo");
      put("task.inputs", "test-system.test-stream");
      put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
      put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
      put("yarn.container.retry.count", "1");
      put("yarn.container.retry.window.ms", "1999999999");
      put("yarn.allocator.sleep.ms", "1");
      put("yarn.container.request.timeout.ms", "2");
    }
  });

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    return new MapConfig(map);
  }

  private Config getConfigWithHostAffinity() {
    Map<String, String> map = new HashMap<>();
    map.putAll(config);
    map.put("yarn.samza.host-affinity.enabled", "true");
    return new MapConfig(map);
  }

  private SamzaAppState state = new SamzaAppState(getCoordinator(1), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2);
  private final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));

  private JobCoordinator getCoordinator(int containerCount) {
    Map<Integer, ContainerModel> containers = new java.util.HashMap<>();
    for (int i = 0; i < containerCount; i++) {
      ContainerModel container = new ContainerModel(i, new HashMap<TaskName, TaskModel>());
      containers.put(i, container);
    }
    Map<Integer, Map<String, String>> localityMap = new HashMap<>();
    localityMap.put(0, new HashMap<String, String>(){{
      put(SetContainerHostMapping.HOST_KEY, "abc");
    }
    });
    LocalityManager mockLocalityManager = mock(LocalityManager.class);
    when(mockLocalityManager.readContainerLocality()).thenReturn(localityMap);

    JobModel jobModel = new JobModel(getConfig(), containers, mockLocalityManager);
    return new JobCoordinator(jobModel, server);
  }

  @Before
  public void setup() throws Exception {
    // Create AMRMClient
    testAMRMClient = new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
            false,
            new ArrayList<Container>(),
            new ArrayList<ContainerStatus>()
        ));
    amRmClientAsync = TestUtil.getAMClient(testAMRMClient);

    // Initialize coordinator url
    state.coordinatorUrl = new URL("http://localhost:1234");
  }

  private Field getPrivateFieldFromTaskManager(String fieldName, SamzaTaskManager object) throws Exception {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  @Test
  public void testSamzaTaskManager() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(getConfig());
    conf.put("yarn.container.memory.mb", "500");
    conf.put("yarn.container.cpu.cores", "5");

    SamzaTaskManager taskManager = new SamzaTaskManager(
        new MapConfig(conf),
        state,
        amRmClientAsync,
        new YarnConfiguration()
        );

    AbstractContainerAllocator allocator = (AbstractContainerAllocator) getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager);
    assertEquals(ContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMaxMemoryMb);
    assertEquals(5, allocator.containerMaxCpuCore);

    conf.clear();
    conf.putAll(getConfigWithHostAffinity());
    conf.put("yarn.container.memory.mb", "500");
    conf.put("yarn.container.cpu.cores", "5");

    taskManager = new SamzaTaskManager(
        new MapConfig(conf),
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );

    allocator = (AbstractContainerAllocator) getPrivateFieldFromTaskManager("containerAllocator", taskManager).get(taskManager);
    assertEquals(HostAwareContainerAllocator.class, allocator.getClass());
    // Asserts that samza exposed container configs is honored by allocator thread
    assertEquals(500, allocator.containerMaxMemoryMb);
    assertEquals(5, allocator.containerMaxCpuCore);
  }

  @Test
  public void testContainerConfigsAreHonoredInAllocator() {

  }

  @Test
  public void testOnInit() throws Exception {
    Config conf = getConfig();
    SamzaTaskManager taskManager = new SamzaTaskManager(
        conf,
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );

    MockContainerAllocator allocator = new MockContainerAllocator(
        amRmClientAsync,
        TestUtil.getContainerUtil(getConfig(), state),
        new YarnConfig(conf));
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, new Thread() {
      public void run() {
        isRunning = true;
      }
    });

    taskManager.onInit();
    Thread.sleep(1000);

    // Verify Allocator thread has started running
    assertTrue(isRunning);

    // Verify the remaining state
    assertEquals(1, state.neededContainers.get());
    assertEquals(1, allocator.requestedContainers);

    taskManager.onShutdown();
  }

  @Test
  public void testOnShutdown() throws Exception {
    SamzaTaskManager taskManager = new SamzaTaskManager(
        getConfig(),
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );
    taskManager.onInit();

    Thread.sleep(100);

    Thread allocatorThread = (Thread) getPrivateFieldFromTaskManager("allocatorThread", taskManager).get(taskManager);
    assertTrue(allocatorThread.isAlive());

    taskManager.onShutdown();

    Thread.sleep(100);
    assertFalse(allocatorThread.isAlive());

  }

  /**
   * Test Task Manager should stop when all containers finish
   */
  @Test
  public void testTaskManagerShouldStopWhenContainersFinish() {
    SamzaTaskManager taskManager = new SamzaTaskManager(
        getConfig(),
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );

    taskManager.onInit();

    assertFalse(taskManager.shouldShutdown());

    taskManager.onContainerCompleted(TestUtil.getContainerStatus(state.amContainerId, ContainerExitStatus.SUCCESS, ""));

    assertTrue(taskManager.shouldShutdown());
  }

  /**
   * Test Task Manager should request a new container when a task fails with unknown exit code
   * When host-affinity is not enabled, it will always request for ANY_HOST
   */
  @Test
  public void testNewContainerRequestedOnFailureWithUnknownCode() throws Exception {
    Config conf = getConfig();
    SamzaTaskManager taskManager = new SamzaTaskManager(
        conf,
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );
    MockContainerAllocator allocator = new MockContainerAllocator(
        amRmClientAsync,
        TestUtil.getContainerUtil(getConfig(), state),
        new YarnConfig(conf));
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // onInit triggers a request
    taskManager.onInit();

    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());

    Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123);
    taskManager.onContainerAllocated(container);

    // Allow container to run and update state
    Thread.sleep(300);

    // Create first container failure
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), 1, "Expecting a failure here"));

    // The above failure should trigger a container request
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());
    assertEquals(ContainerRequestState.ANY_HOST, allocator.containerRequestState.getRequestsQueue().peek().getPreferredHost());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());

    taskManager.onContainerAllocated(container);

    // Allow container to run and update state
    Thread.sleep(300);

    assertTrue(state.jobHealthy.get());

    // Create a second failure
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), 1, "Expecting a failure here"));

    // The above failure should trigger a job shutdown because our retry count is set to 1
    assertEquals(0, allocator.containerRequestState.getRequestsQueue().size());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());
    assertFalse(state.jobHealthy.get());
    assertTrue(taskManager.shouldShutdown());
    assertEquals(FinalApplicationStatus.FAILED, state.status);

    taskManager.onShutdown();
  }

  /**
   * Test Task Manager should request a new container when a task fails with unknown exit code
   * When host-affinity is enabled, it will always request for the same host that it was last seen on
   */
  @Test
  public void testSameContainerRequestedOnFailureWithUnknownCode() throws Exception {
    Config conf = getConfigWithHostAffinity();
    SamzaTaskManager taskManager = new SamzaTaskManager(
        conf,
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );
    MockContainerAllocator allocator = new MockContainerAllocator(
        amRmClientAsync,
        TestUtil.getContainerUtil(getConfig(), state),
        new YarnConfig(conf));
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // onInit triggers a request
    taskManager.onInit();

    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());

    Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123);
    taskManager.onContainerAllocated(container);

    // Allow container to run and update state
    Thread.sleep(300);

    // Create first container failure
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), 1, "Expecting a failure here"));

    // The above failure should trigger a container request
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());
    assertEquals("abc", allocator.containerRequestState.getRequestsQueue().peek().getPreferredHost());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());

    taskManager.onContainerAllocated(container);

    // Allow container to run and update state
    Thread.sleep(300);

    assertTrue(state.jobHealthy.get());

    // Create a second failure
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), 1, "Expecting a failure here"));

    // The above failure should trigger a job shutdown because our retry count is set to 1
    assertEquals(0, allocator.containerRequestState.getRequestsQueue().size());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());
    assertFalse(state.jobHealthy.get());
    assertTrue(taskManager.shouldShutdown());
    assertEquals(FinalApplicationStatus.FAILED, state.status);

    taskManager.onShutdown();
  }

  /**
   * Test AM requests a new container when a task fails
   * Error codes with same behavior - Disk failure, preemption and aborted
   */
  @Test
  public void testNewContainerRequestedOnFailureWithKnownCode() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.putAll(getConfig());
    config.remove("yarn.container.retry.count");

    SamzaTaskManager taskManager = new SamzaTaskManager(
        new MapConfig(config),
        state,
        amRmClientAsync,
        new YarnConfiguration()
    );
    MockContainerAllocator allocator = new MockContainerAllocator(
        amRmClientAsync,
        TestUtil.getContainerUtil(getConfig(), state),
        new YarnConfig(new MapConfig(config)));
    getPrivateFieldFromTaskManager("containerAllocator", taskManager).set(taskManager, allocator);

    Thread thread = new Thread(allocator);
    getPrivateFieldFromTaskManager("allocatorThread", taskManager).set(taskManager, thread);

    // Start the task manager
    taskManager.onInit();
    assertFalse(taskManager.shouldShutdown());
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());

    Container container = TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "abc", 123);
    taskManager.onContainerAllocated(container);

    // Allow container to run and update state
    Thread.sleep(300);

    // Create container failure - with ContainerExitStatus.DISKS_FAILED
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), ContainerExitStatus.DISKS_FAILED, "Disk failure"));

    // The above failure should trigger a container request
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());
    assertEquals(ContainerRequestState.ANY_HOST, allocator.containerRequestState.getRequestsQueue().peek().getPreferredHost());

    // Create container failure - with ContainerExitStatus.PREEMPTED
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), ContainerExitStatus.PREEMPTED, "Task Preempted by RM"));

    // The above failure should trigger a container request
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ContainerRequestState.ANY_HOST, allocator.containerRequestState.getRequestsQueue().peek().getPreferredHost());

    // Create container failure - with ContainerExitStatus.ABORTED
    taskManager.onContainerCompleted(TestUtil.getContainerStatus(container.getId(), ContainerExitStatus.ABORTED, "Task Aborted by the NM"));

    // The above failure should trigger a container request
    assertEquals(1, allocator.containerRequestState.getRequestsQueue().size());
    assertEquals(2, testAMRMClient.requests.size());
    assertEquals(0, testAMRMClient.getRelease().size());
    assertFalse(taskManager.shouldShutdown());
    assertFalse(state.jobHealthy.get());
    assertEquals(ContainerRequestState.ANY_HOST, allocator.containerRequestState.getRequestsQueue().peek().getPreferredHost());

    taskManager.onShutdown();
  }
}