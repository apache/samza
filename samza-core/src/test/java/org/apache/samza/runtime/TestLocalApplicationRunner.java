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

package org.apache.samza.runtime;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.CoordinationUtilsFactory;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.doReturn;


@RunWith(PowerMockRunner.class)
@PrepareForTest(LocalApplicationRunner.class)
public class TestLocalApplicationRunner {

  private static final String PLAN_JSON =
      "{" + "\"jobs\":[{" + "\"jobName\":\"test-application\"," + "\"jobId\":\"1\"," + "\"operatorGraph\":{"
          + "\"intermediateStreams\":{%s}," + "\"applicationName\":\"test-application\",\"applicationId\":\"1\"}";
  private static final String STREAM_SPEC_JSON_FORMAT =
      "\"%s\":{" + "\"streamSpec\":{" + "\"id\":\"%s\"," + "\"systemName\":\"%s\"," + "\"physicalName\":\"%s\","
          + "\"partitionCount\":2}," + "\"sourceJobs\":[\"test-app\"]," + "\"targetJobs\":[\"test-target-app\"]},";

  @Test
  public void testStreamCreation()
      throws Exception {
    Map<String, String> config = new HashMap<>();
    LocalApplicationRunner runner = spy(new LocalApplicationRunner(new MapConfig(config)));
    StreamApplication app = mock(StreamApplication.class);

    StreamManager streamManager = mock(StreamManager.class);
    doReturn(streamManager).when(runner).getStreamManager();

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system")));
    when(plan.getPlanAsJson()).thenReturn("");
    doReturn(plan).when(runner).getExecutionPlan(any());

    CoordinationUtilsFactory coordinationUtilsFactory = mock(CoordinationUtilsFactory.class);
    JobCoordinatorConfig mockJcConfig = mock(JobCoordinatorConfig.class);
    when(mockJcConfig.getCoordinationUtilsFactory()).thenReturn(coordinationUtilsFactory);
    PowerMockito.whenNew(JobCoordinatorConfig.class).withAnyArguments().thenReturn(mockJcConfig);

    try {
      runner.run(app);
      runner.waitForFinish();
    } catch (Throwable t) {
      assertNotNull(t); //no jobs exception
    }

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(streamManager).createStreams(captor.capture());
    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
  }

  @Test
  public void testStreamCreationWithCoordination()
      throws Exception {
    Map<String, String> config = new HashMap<>();
    LocalApplicationRunner localRunner = new LocalApplicationRunner(new MapConfig(config));
    LocalApplicationRunner runner = spy(localRunner);

    StreamApplication app = mock(StreamApplication.class);

    StreamManager streamManager = mock(StreamManager.class);
    doReturn(streamManager).when(runner).getStreamManager();

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system")));
    when(plan.getPlanAsJson()).thenReturn("");
    doReturn(plan).when(runner).getExecutionPlan(any());

    CoordinationUtils coordinationUtils = mock(CoordinationUtils.class);
    CoordinationUtilsFactory coordinationUtilsFactory = mock(CoordinationUtilsFactory.class);
    JobCoordinatorConfig mockJcConfig = mock(JobCoordinatorConfig.class);
    when(mockJcConfig.getCoordinationUtilsFactory()).thenReturn(coordinationUtilsFactory);
    PowerMockito.whenNew(JobCoordinatorConfig.class).withAnyArguments().thenReturn(mockJcConfig);

    DistributedLockWithState lock = mock(DistributedLockWithState.class);
    when(lock.lockIfNotSet(anyLong(), anyObject())).thenReturn(true);
    when(coordinationUtils.getLockWithState(anyString())).thenReturn(lock);
    when(coordinationUtilsFactory.getCoordinationUtils(anyString(), anyString(), anyObject()))
        .thenReturn(coordinationUtils);

    try {
      runner.run(app);
      runner.waitForFinish();
    } catch (Throwable t) {
      assertNotNull(t); //no jobs exception
    }

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(streamManager).createStreams(captor.capture());

    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
  }

  @Test
  public void testRunStreamTask()
      throws Exception {
    final Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config.put(TaskConfig.TASK_CLASS(), "org.apache.samza.task.IdentityStreamTask");

    LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessorLifecycleListener> captor =
        ArgumentCaptor.forClass(StreamProcessorLifecycleListener.class);

    doAnswer(i ->
      {
        StreamProcessorLifecycleListener listener = captor.getValue();
        listener.onStart();
        listener.onShutdown();
        return null;
      }).when(sp).start();

    LocalApplicationRunner spy = spy(runner);
    doReturn(sp).when(spy).createStreamProcessor(any(Config.class), captor.capture());

    spy.runTask();

    assertEquals(ApplicationStatus.SuccessfulFinish, spy.status(null));
  }

  @Test
  public void testRunComplete()
      throws Exception {
    final Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    LocalApplicationRunner runner = spy(new LocalApplicationRunner(new MapConfig(config)));
    StreamApplication app = mock(StreamApplication.class);

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(Collections.emptyList());
    when(plan.getPlanAsJson()).thenReturn("");
    when(plan.getJobConfigs()).thenReturn(Collections.singletonList(new JobConfig(new MapConfig(config))));
    doReturn(plan).when(runner).getExecutionPlan(any());

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessorLifecycleListener> captor =
        ArgumentCaptor.forClass(StreamProcessorLifecycleListener.class);

    doAnswer(i ->
      {
        StreamProcessorLifecycleListener listener = captor.getValue();
        listener.onStart();
        listener.onShutdown();
        return null;
      }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    runner.run(app);
    runner.waitForFinish();

    assertEquals(runner.status(app), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunFailure()
      throws Exception {
    final Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.PROCESSOR_ID, "0");
    LocalApplicationRunner runner = spy(new LocalApplicationRunner(new MapConfig(config)));
    StreamApplication app = mock(StreamApplication.class);

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(Collections.emptyList());
    when(plan.getPlanAsJson()).thenReturn("");
    when(plan.getJobConfigs()).thenReturn(Collections.singletonList(new JobConfig(new MapConfig(config))));
    doReturn(plan).when(runner).getExecutionPlan(any());

    Throwable t = new Throwable("test failure");
    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessorLifecycleListener> captor =
        ArgumentCaptor.forClass(StreamProcessorLifecycleListener.class);

    doAnswer(i ->
      {
        StreamProcessorLifecycleListener listener = captor.getValue();
        listener.onFailure(t);
        return null;
      }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    try {
      runner.run(app);
      runner.waitForFinish();
    } catch (Throwable th) {
      assertNotNull(th);
    }

    assertEquals(runner.status(app), ApplicationStatus.UnsuccessfulFinish);
  }

  public static Set<StreamProcessor> getProcessors(LocalApplicationRunner runner) {
    return runner.getProcessors();
  }

  /**
   * A test case to verify if the plan results in different hash if there is change in topological sort order.
   * Note: the overall JOB PLAN remains the same outside the scope of intermediate streams the sake of these test cases.
   */
  @Test
  public void testPlanIdWithShuffledStreamSpecs() {
    List<StreamSpec> streamSpecs = ImmutableList.of(new StreamSpec("test-stream-1", "stream-1", "testStream"),
        new StreamSpec("test-stream-2", "stream-2", "testStream"),
        new StreamSpec("test-stream-3", "stream-3", "testStream"));
    String planIdBeforeShuffle = getExecutionPlanId(streamSpecs);

    List<StreamSpec> shuffledStreamSpecs = ImmutableList.of(new StreamSpec("test-stream-2", "stream-2", "testStream"),
        new StreamSpec("test-stream-1", "stream-1", "testStream"),
        new StreamSpec("test-stream-3", "stream-3", "testStream"));


    assertFalse("Expected both of the latch ids to be different",
        planIdBeforeShuffle.equals(getExecutionPlanId(shuffledStreamSpecs)));
  }

  /**
   * A test case to verify if the plan results in same hash in case of same plan.
   * Note: the overall JOB PLAN remains the same outside the scope of intermediate streams the sake of these test cases.
   */
  @Test
  public void testGeneratePlanIdWithSameStreamSpecs() {
    List<StreamSpec> streamSpecs = ImmutableList.of(new StreamSpec("test-stream-1", "stream-1", "testStream"),
        new StreamSpec("test-stream-2", "stream-2", "testStream"),
        new StreamSpec("test-stream-3", "stream-3", "testStream"));
    String planIdForFirstAttempt = getExecutionPlanId(streamSpecs);
    String planIdForSecondAttempt = getExecutionPlanId(streamSpecs);

    assertEquals("Expected latch ids to match!", "1447946713", planIdForFirstAttempt);
    assertEquals("Expected latch ids to match for the second attempt!", planIdForFirstAttempt, planIdForSecondAttempt);
  }

  /**
   * A test case to verify plan results in different hash in case of different intermediate stream.
   * Note: the overall JOB PLAN remains the same outside the scope of intermediate streams the sake of these test cases.
   */
  @Test
  public void testGeneratePlanIdWithDifferentStreamSpecs() {
    List<StreamSpec> streamSpecs = ImmutableList.of(new StreamSpec("test-stream-1", "stream-1", "testStream"),
        new StreamSpec("test-stream-2", "stream-2", "testStream"),
        new StreamSpec("test-stream-3", "stream-3", "testStream"));
    String planIdBeforeShuffle = getExecutionPlanId(streamSpecs);

    List<StreamSpec> updatedStreamSpecs = ImmutableList.of(new StreamSpec("test-stream-1", "stream-1", "testStream"),
        new StreamSpec("test-stream-4", "stream-4", "testStream"),
        new StreamSpec("test-stream-3", "stream-3", "testStream"));


    assertFalse("Expected both of the latch ids to be different",
        planIdBeforeShuffle.equals(getExecutionPlanId(updatedStreamSpecs)));
  }

  private String getExecutionPlanId(List<StreamSpec> updatedStreamSpecs) {
    String intermediateStreamJson =
        updatedStreamSpecs.stream().map(this::streamSpecToJson).collect(Collectors.joining(","));

    int planId = String.format(PLAN_JSON, intermediateStreamJson).hashCode();

    return String.valueOf(planId);
  }

  private String streamSpecToJson(StreamSpec streamSpec) {
    return String.format(STREAM_SPEC_JSON_FORMAT, streamSpec.getId(), streamSpec.getId(), streamSpec.getSystemName(),
        streamSpec.getPhysicalName());
  }
}
