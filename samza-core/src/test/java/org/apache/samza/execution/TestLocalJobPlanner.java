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
package org.apache.samza.execution;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.CoordinationUtilsFactory;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link LocalJobPlanner}
 *
 * TODO: consolidate this with unit tests for ExecutionPlanner after SAMZA-1811
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalJobPlanner.class, JobCoordinatorConfig.class})
public class TestLocalJobPlanner {

  private static final String PLAN_JSON =
      "{" + "\"jobs\":[{" + "\"jobName\":\"test-application\"," + "\"jobId\":\"1\"," + "\"operatorGraph\":{"
          + "\"intermediateStreams\":{%s}," + "\"applicationName\":\"test-application\",\"applicationId\":\"1\"}";
  private static final String STREAM_SPEC_JSON_FORMAT =
      "\"%s\":{" + "\"streamSpec\":{" + "\"id\":\"%s\"," + "\"systemName\":\"%s\"," + "\"physicalName\":\"%s\","
          + "\"partitionCount\":2}," + "\"sourceJobs\":[\"test-app\"]," + "\"targetJobs\":[\"test-target-app\"]},";

  private LocalJobPlanner localPlanner;

  @Test
  public void testStreamCreation()
      throws Exception {
    localPlanner = createLocalJobPlanner(mock(StreamApplicationDescriptorImpl.class));
    StreamManager streamManager = mock(StreamManager.class);
    doReturn(streamManager).when(localPlanner).buildAndStartStreamManager(any(Config.class));

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(
        Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system")));
    when(plan.getPlanAsJson()).thenReturn("");
    when(plan.getJobConfigs()).thenReturn(Collections.singletonList(mock(JobConfig.class)));
    doReturn(plan).when(localPlanner).getExecutionPlan(any());

    CoordinationUtilsFactory coordinationUtilsFactory = mock(CoordinationUtilsFactory.class);
    JobCoordinatorConfig mockJcConfig = mock(JobCoordinatorConfig.class);
    when(mockJcConfig.getCoordinationUtilsFactory()).thenReturn(coordinationUtilsFactory);
    PowerMockito.whenNew(JobCoordinatorConfig.class).withAnyArguments().thenReturn(mockJcConfig);

    localPlanner.prepareJobs();

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(streamManager).createStreams(captor.capture());
    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
    verify(streamManager).stop();
  }

  @Test
  public void testStreamCreationWithCoordination()
      throws Exception {
    localPlanner = createLocalJobPlanner(mock(StreamApplicationDescriptorImpl.class));
    StreamManager streamManager = mock(StreamManager.class);
    doReturn(streamManager).when(localPlanner).buildAndStartStreamManager(any(Config.class));

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system")));
    when(plan.getPlanAsJson()).thenReturn("");
    when(plan.getJobConfigs()).thenReturn(Collections.singletonList(mock(JobConfig.class)));
    doReturn(plan).when(localPlanner).getExecutionPlan(any());

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

    localPlanner.prepareJobs();

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(streamManager).createStreams(captor.capture());

    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
    verify(streamManager).stop();
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

  private LocalJobPlanner createLocalJobPlanner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc) throws  Exception {
    CoordinationUtils coordinationUtils = mock(CoordinationUtils.class);
    DistributedLockWithState lock = mock(DistributedLockWithState.class);
    when(lock.lockIfNotSet(anyLong(), anyObject())).thenReturn(true);
    when(coordinationUtils.getLockWithState(anyString())).thenReturn(lock);

    return spy(new LocalJobPlanner(appDesc, coordinationUtils, "FAKE_UID", "FAKE_RUNID"));
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
