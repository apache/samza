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
package org.apache.samza.webapp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaResource;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GroupByContainerCount;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.job.yarn.SamzaAppMasterMetrics;
import org.apache.samza.job.yarn.YarnAppState;
import org.apache.samza.job.yarn.YarnContainer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestApplicationMasterRestClient {
  private static final String AM_HOST_NAME = "dummyHost";
  private static final int AM_RPC_PORT = 1337;
  private static final int AM_HTTP_PORT = 7001;
  private static final String YARN_CONTAINER_ID_1 = "container_e38_1510966221296_0007_01_000001";
  private static final String YARN_CONTAINER_ID_2 = "container_e38_1510966221296_0007_01_000002";
  private static final String YARN_CONTAINER_ID_3 = "container_e38_1510966221296_0007_01_000003";
  private static final String APP_ATTEMPT_ID = "appattempt_1510966221296_0007_000001";

  private final ObjectMapper jsonMapper = SamzaObjectMapper.getObjectMapper();

  private CloseableHttpClient mockClient;

  @Before
  public void setup() {
    mockClient = mock(CloseableHttpClient.class);
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none(); // Enables us to verify the exception message

  @Test
  public void testGetMetricsSuccess() throws IOException {
    SamzaApplicationState samzaAppState = createSamzaApplicationState();

    MetricsRegistryMap registry = new MetricsRegistryMap();
    assignMetricValues(samzaAppState, registry);

    String response = ApplicationMasterRestServlet.getMetrics(jsonMapper, registry);
    setupMockClientResponse(HttpStatus.SC_OK, "Success", response);

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    Map<String, Map<String, Object>> metricsResult = client.getMetrics();

    String group = SamzaAppMasterMetrics.class.getCanonicalName();
    assertEquals(1, metricsResult.size());
    assertTrue(metricsResult.containsKey(group));

    Map<String, Object> amMetricsGroup = metricsResult.get(group);
    assertEquals(7, amMetricsGroup.size());
    assertEquals(samzaAppState.runningContainers.size(),  amMetricsGroup.get("running-containers"));
    assertEquals(samzaAppState.neededContainers.get(),    amMetricsGroup.get("needed-containers"));
    assertEquals(samzaAppState.completedContainers.get(), amMetricsGroup.get("completed-containers"));
    assertEquals(samzaAppState.failedContainers.get(),    amMetricsGroup.get("failed-containers"));
    assertEquals(samzaAppState.releasedContainers.get(),  amMetricsGroup.get("released-containers"));
    assertEquals(samzaAppState.containerCount.get(),      amMetricsGroup.get("container-count"));
    assertEquals(samzaAppState.jobHealthy.get() ? 1 : 0,  amMetricsGroup.get("job-healthy"));
  }

  @Test
  public void testGetMetricsError() throws IOException {
    setupErrorTest("metrics");

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    client.getMetrics();
  }

  @Test
  public void testGetTaskContextSuccess() throws IOException {
    ContainerId containerId = ConverterUtils.toContainerId(YARN_CONTAINER_ID_1);
    YarnAppState yarnAppState = createYarnAppState(containerId);

    String response = ApplicationMasterRestServlet.getTaskContext(jsonMapper, yarnAppState);
    setupMockClientResponse(HttpStatus.SC_OK, "Success", response);

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    Map<String, Object> taskContextResult = client.getTaskContext();

    assertEquals(2, taskContextResult.size());
    assertEquals(2, taskContextResult.get("task-id"));
    assertEquals(containerId.toString(), taskContextResult.get("name"));
  }

  @Test
  public void testTaskContextError() throws IOException {
    setupErrorTest("task context");

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    client.getTaskContext();
  }

  @Test
  public void testGetAmStateSuccess() throws IOException {
    SamzaApplicationState samzaAppState = createSamzaApplicationState();

    ApplicationAttemptId attemptId = ConverterUtils.toApplicationAttemptId(APP_ATTEMPT_ID);
    ContainerId containerId = ConverterUtils.toContainerId(YARN_CONTAINER_ID_1);
    YarnAppState yarnAppState = createYarnAppState(containerId);

    String response = ApplicationMasterRestServlet.getAmState(jsonMapper, samzaAppState, yarnAppState);
    setupMockClientResponse(HttpStatus.SC_OK, "Success", response);

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    Map<String, Object> amStateResult = client.getAmState();

    assertEquals(4, amStateResult.size());
    assertEquals(String.format("%s:%s", yarnAppState.nodeHost, yarnAppState.rpcUrl.getPort()), amStateResult.get("host"));
    assertEquals(containerId.toString(), amStateResult.get("container-id"));
    // Can only validate the keys because up-time changes everytime it's requested
    assertEquals(buildExpectedContainerResponse(yarnAppState.runningYarnContainers, samzaAppState).keySet(),
        ((Map<String, Object>) amStateResult.get("containers")).keySet());
    assertEquals(attemptId.toString(), amStateResult.get("app-attempt-id"));
  }

  @Test
  public void testGetAmStateError() throws IOException {
    setupErrorTest("AM state");

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    client.getAmState();
  }

  @Test
  public void testGetConfigSuccess() throws IOException {
    SamzaApplicationState samzaAppState = createSamzaApplicationState();

    Map<String, String> configMap = ImmutableMap.of("key1", "value1", "key2", "value2");
    Config config = new MapConfig(configMap);

    String response = ApplicationMasterRestServlet.getConfig(jsonMapper, config);
    setupMockClientResponse(HttpStatus.SC_OK, "Success", response);

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    Map<String, Object> configResult = client.getConfig();

    assertEquals(configMap, configResult);
  }

  @Test
  public void testGetConfigError() throws IOException {
    setupErrorTest("config");

    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    client.getConfig();
  }

  @Test
  public void testCloseMethodClosesHttpClient() throws IOException {
    ApplicationMasterRestClient client = new ApplicationMasterRestClient(mockClient, AM_HOST_NAME, AM_RPC_PORT);
    client.close();

    verify(mockClient).close();
  }

  private void setupMockClientResponse(int statusCode, String statusReason, String responseBody) throws IOException {
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(statusCode);
    when(statusLine.getReasonPhrase()).thenReturn(statusReason);

    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContent()).thenReturn(new ReaderInputStream(new StringReader(responseBody)));

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(response.getEntity()).thenReturn(entity);

    when(mockClient.execute(any(HttpHost.class), any(HttpGet.class))).thenReturn(response);
  }

  private SamzaApplicationState createSamzaApplicationState() {
    HashMap<String, ContainerModel> containers = generateContainers();

    JobModel mockJobModel = mock(JobModel.class);
    when(mockJobModel.getContainers()).thenReturn(containers);
    JobModelManager mockJobModelManager = mock(JobModelManager.class);
    when(mockJobModelManager.jobModel()).thenReturn(mockJobModel);

    SamzaApplicationState samzaApplicationState = new SamzaApplicationState(mockJobModelManager);

    samzaApplicationState.runningContainers.put(YARN_CONTAINER_ID_3,
        new SamzaResource(1, 2, "dummyNodeHost1", "dummyResourceId1"));
    samzaApplicationState.runningContainers.put(YARN_CONTAINER_ID_2,
        new SamzaResource(2, 4, "dummyNodeHost2", "dummyResourceId2"));
    return samzaApplicationState;
  }

  private YarnAppState createYarnAppState(ContainerId containerId) throws MalformedURLException {
    YarnAppState yarnAppState = new YarnAppState(2, containerId, AM_HOST_NAME, AM_RPC_PORT, AM_HTTP_PORT);
    yarnAppState.rpcUrl = new URL(new HttpHost(AM_HOST_NAME, AM_RPC_PORT).toURI());
    yarnAppState.runningYarnContainers.put("0", new YarnContainer(Container.newInstance(
        ConverterUtils.toContainerId(YARN_CONTAINER_ID_2),
        ConverterUtils.toNodeIdWithDefaultPort("dummyNodeHost1"),
        "dummyNodeHttpHost1",
        null,
        null,
        null
    )));
    yarnAppState.runningYarnContainers.put("1", new YarnContainer(Container.newInstance(
        ConverterUtils.toContainerId(YARN_CONTAINER_ID_3),
        ConverterUtils.toNodeIdWithDefaultPort("dummyNodeHost2"),
        "dummyNodeHttpHost2",
        null,
        null,
        null
    )));
    return yarnAppState;
  }

  private HashMap<String, ContainerModel> generateContainers() {
    Set<TaskModel> taskModels = ImmutableSet.of(
        new TaskModel(new TaskName("task1"),
                      ImmutableSet.of(new SystemStreamPartition(new SystemStream("system1", "stream1"), new Partition(0))),
                      new Partition(0)),
        new TaskModel(new TaskName("task2"),
            ImmutableSet.of(new SystemStreamPartition(new SystemStream("system1", "stream1"), new Partition(1))),
            new Partition(1)));
    GroupByContainerCount grouper = new GroupByContainerCount(2);
    Set<ContainerModel> containerModels = grouper.group(taskModels);
    HashMap<String, ContainerModel> containers = new HashMap<>();
    for (ContainerModel containerModel : containerModels) {
      containers.put(containerModel.getProcessorId(), containerModel);
    }
    return containers;
  }

  private Map<String, Map<String, Object>> buildExpectedContainerResponse(Map<String, YarnContainer> runningYarnContainers,
      SamzaApplicationState samzaAppState) throws IOException {
    Map<String, Map<String, Object>> containers = new HashMap<>();

    runningYarnContainers.forEach((containerId, container) -> {
      String yarnContainerId = container.id().toString();
      Map<String, Object> containerMap = new HashMap();
      Map<TaskName, TaskModel> taskModels = samzaAppState.jobModelManager.jobModel().getContainers().get(containerId).getTasks();
      containerMap.put("yarn-address", container.nodeHttpAddress());
      containerMap.put("start-time", String.valueOf(container.startTime()));
      containerMap.put("up-time", String.valueOf(container.upTime()));
      containerMap.put("task-models", taskModels);
      containerMap.put("container-id", containerId);
      containers.put(yarnContainerId, containerMap);
    });

    return jsonMapper.readValue(jsonMapper.writeValueAsString(containers), new TypeReference<Map<String, Map<String, Object>>>() {});
  }

  private void assignMetricValues(SamzaApplicationState samzaAppState, MetricsRegistryMap registry) {
    SamzaAppMasterMetrics metrics = new SamzaAppMasterMetrics(new MapConfig(), samzaAppState, registry);
    metrics.start();
    samzaAppState.runningContainers.put("dummyContainer",
        new SamzaResource(1, 2, AM_HOST_NAME, "dummyResourceId")); // 1 container
    samzaAppState.neededContainers.set(2);
    samzaAppState.completedContainers.set(3);
    samzaAppState.failedContainers.set(4);
    samzaAppState.releasedContainers.set(5);
    samzaAppState.containerCount.set(6);
    samzaAppState.jobHealthy.set(true);
  }

  private void setupErrorTest(String entityToFetch) throws IOException {
    String statusReason = "Dummy status reason";
    expectedException.expect(SamzaException.class);
    expectedException.expectMessage(String.format(
        "Error retrieving %s from host %s. Response: %s",
        entityToFetch,
        new HttpHost(AM_HOST_NAME, AM_RPC_PORT).toURI(),
        statusReason));

    setupMockClientResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, statusReason, "");
  }
}
