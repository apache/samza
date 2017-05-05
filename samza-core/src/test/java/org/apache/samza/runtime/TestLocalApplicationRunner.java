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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestLocalApplicationRunner {

  @Test
  public void testStreamCreation() throws Exception {
    Map<String, String> config = new HashMap<>();
    LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));
    StreamApplication app = mock(StreamApplication.class);
    doNothing().when(app).init(anyObject(), anyObject());

    ExecutionPlanner planner = mock(ExecutionPlanner.class);
    Field plannerField = runner.getClass().getSuperclass().getDeclaredField("planner");
    plannerField.setAccessible(true);
    plannerField.set(runner, planner);

    StreamManager streamManager = mock(StreamManager.class);
    Field streamManagerField = runner.getClass().getSuperclass().getDeclaredField("streamManager");
    streamManagerField.setAccessible(true);
    streamManagerField.set(runner, streamManager);
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

    ExecutionPlan plan = new ExecutionPlan() {
      @Override
      public List<JobConfig> getJobConfigs() {
        return Collections.emptyList();
      }

      @Override
      public List<StreamSpec> getIntermediateStreams() {
        return Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system"));
      }

      @Override
      public String getPlanAsJson()
          throws Exception {
        return "";
      }
    };
    when(planner.plan(anyObject())).thenReturn(plan);

    LocalApplicationRunner spy = spy(runner);
    try {
      spy.run(app);
    } catch (Throwable t) {
      assertNotNull(t); //no jobs exception
    }

    verify(streamManager).createStreams(captor.capture());
    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
  }

  @Test
  public void testStreamCreationWithCoordination() throws Exception {
    Map<String, String> config = new HashMap<>();
    LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));
    StreamApplication app = mock(StreamApplication.class);
    doNothing().when(app).init(anyObject(), anyObject());

    ExecutionPlanner planner = mock(ExecutionPlanner.class);
    Field plannerField = runner.getClass().getSuperclass().getDeclaredField("planner");
    plannerField.setAccessible(true);
    plannerField.set(runner, planner);

    StreamManager streamManager = mock(StreamManager.class);
    Field streamManagerField = runner.getClass().getSuperclass().getDeclaredField("streamManager");
    streamManagerField.setAccessible(true);
    streamManagerField.set(runner, streamManager);
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

    ExecutionPlan plan = new ExecutionPlan() {
      @Override
      public List<JobConfig> getJobConfigs() {
        return Collections.emptyList();
      }

      @Override
      public List<StreamSpec> getIntermediateStreams() {
        return Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system"));
      }

      @Override
      public String getPlanAsJson()
          throws Exception {
        return "";
      }
    };
    when(planner.plan(anyObject())).thenReturn(plan);

    LocalApplicationRunner spy = spy(runner);

    CoordinationUtils coordinationUtils = mock(CoordinationUtils.class);
    LeaderElector leaderElector = new LeaderElector() {
      private LeaderElectorListener leaderElectorListener;

      @Override
      public void setLeaderElectorListener(LeaderElectorListener listener) {
        this.leaderElectorListener = listener;
      }

      @Override
      public void tryBecomeLeader() {
        leaderElectorListener.onBecomingLeader();
      }

      @Override
      public void resignLeadership() {}

      @Override
      public boolean amILeader() {
        return false;
      }
    };

    Latch latch = new Latch() {
      boolean done = false;
      @Override
      public void await(long timeout, TimeUnit tu)
          throws TimeoutException {
        // in this test, latch is released before wait
        assertTrue(done);
      }

      @Override
      public void countDown() {
        done = true;
      }
    };
    when(coordinationUtils.getLeaderElector()).thenReturn(leaderElector);
    when(coordinationUtils.getLatch(anyInt(), anyString())).thenReturn(latch);
    doReturn(coordinationUtils).when(spy).createCoordinationUtils();

    try {
      spy.run(app);
    } catch (Throwable t) {
      assertNotNull(t); //no jobs exception
    }

    verify(streamManager).createStreams(captor.capture());
    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
  }

  @Test
  public void testRunComplete() throws Exception {
    final Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));
    StreamApplication app = mock(StreamApplication.class);
    doNothing().when(app).init(anyObject(), anyObject());

    ExecutionPlanner planner = mock(ExecutionPlanner.class);
    Field plannerField = runner.getClass().getSuperclass().getDeclaredField("planner");
    plannerField.setAccessible(true);
    plannerField.set(runner, planner);

    ExecutionPlan plan = new ExecutionPlan() {
      @Override
      public List<JobConfig> getJobConfigs() {
        return Collections.singletonList(new JobConfig(new MapConfig(config)));
      }

      @Override
      public List<StreamSpec> getIntermediateStreams() {
        return Collections.emptyList();
      }

      @Override
      public String getPlanAsJson()
          throws Exception {
        return "";
      }
    };
    when(planner.plan(anyObject())).thenReturn(plan);

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
    doReturn(sp).when(spy).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    spy.run(app);

    assertEquals(spy.status(app), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunFailure() throws Exception {
    final Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.PROCESSOR_ID, "0");
    LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));
    StreamApplication app = mock(StreamApplication.class);
    doNothing().when(app).init(anyObject(), anyObject());

    ExecutionPlanner planner = mock(ExecutionPlanner.class);
    Field plannerField = runner.getClass().getSuperclass().getDeclaredField("planner");
    plannerField.setAccessible(true);
    plannerField.set(runner, planner);

    ExecutionPlan plan = new ExecutionPlan() {
      @Override
      public List<JobConfig> getJobConfigs() {
        return Collections.singletonList(new JobConfig(new MapConfig(config)));
      }

      @Override
      public List<StreamSpec> getIntermediateStreams() {
        return Collections.emptyList();
      }

      @Override
      public String getPlanAsJson()
          throws Exception {
        return "";
      }
    };
    when(planner.plan(anyObject())).thenReturn(plan);

    Throwable t = new Throwable("test failure");
    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessorLifecycleListener> captor =
        ArgumentCaptor.forClass(StreamProcessorLifecycleListener.class);

    doAnswer(i -> {
        StreamProcessorLifecycleListener listener = captor.getValue();
        listener.onFailure(t);
        return null;
      }).when(sp).start();


    LocalApplicationRunner spy = spy(runner);
    doReturn(sp).when(spy).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    try {
      spy.run(app);
    } catch (Throwable th) {
      assertNotNull(th);
    }

    assertEquals(spy.status(app), ApplicationStatus.UnsuccessfulFinish);
  }

}
