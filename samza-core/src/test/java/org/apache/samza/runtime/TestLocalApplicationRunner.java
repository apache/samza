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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.task.IdentityStreamTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class TestLocalApplicationRunner {

  private Config config;
  private SamzaApplication mockApp;
  private LocalApplicationRunner runner;
  private LocalJobPlanner localPlanner;

  @Before
  public void setUp() {
    config = new MapConfig();
    mockApp = mock(StreamApplication.class);
    prepareTest();
  }

  @Test
  public void testRunStreamTask()
      throws Exception {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(JobConfig.JOB_NAME(), "test-task-job");
    cfgs.put(JobConfig.JOB_ID(), "jobId");
    config = new MapConfig(cfgs);
    mockApp = new LegacyTaskApplication(IdentityStreamTask.class.getName());
    prepareTest();

    StreamProcessor sp = mock(StreamProcessor.class);

    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i ->
      {
        ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
        listener.afterStart();
        listener.afterStop();
        return null;
      }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture());
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).status();

    runner.run();

    assertEquals(ApplicationStatus.SuccessfulFinish, runner.status());
  }

  @Test
  public void testRunComplete()
      throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> {
      appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    };
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i ->
      {
        ProcessorLifecycleListener listener = captor.getValue().createInstance(sp);
        listener.afterStart();
        listener.afterStop();
        return null;
      }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    runner.run();
    runner.waitForFinish();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunFailure()
      throws Exception {
    Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.PROCESSOR_ID, "0");
    config = new MapConfig(cfgs);
    ProcessorLifecycleListenerFactory mockFactory = (pContext, cfg) -> mock(ProcessorLifecycleListener.class);
    mockApp = (StreamApplication) appDesc -> {
      appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    };
    prepareTest();

    // return the jobConfigs from the planner
    doReturn(Collections.singletonList(new JobConfig(new MapConfig(config)))).when(localPlanner).prepareJobs();

    StreamProcessor sp = mock(StreamProcessor.class);
    ArgumentCaptor<StreamProcessor.StreamProcessorLifecycleListenerFactory> captor =
        ArgumentCaptor.forClass(StreamProcessor.StreamProcessorLifecycleListenerFactory.class);

    doAnswer(i ->
      {
        throw new Exception("test failure");
      }).when(sp).start();

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture());

    try {
      runner.run();
      runner.waitForFinish();
    } catch (Throwable th) {
      assertNotNull(th);
    }

    assertEquals(runner.status(), ApplicationStatus.UnsuccessfulFinish);
  }

  @Test
  public void testWaitForFinishReturnsBeforeTimeout() {
    long timeoutInMs = 1000;

    runner.getShutdownLatch().countDown();
    boolean finished = runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    assertTrue("Application did not finish before the timeout.", finished);
  }

  @Test
  public void testWaitForFinishTimesout() {
    long timeoutInMs = 100;
    boolean finished = runner.waitForFinish(Duration.ofMillis(timeoutInMs));
    assertFalse("Application finished before the timeout.", finished);
  }

  private void prepareTest() {
    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(mockApp, config);
    localPlanner = spy(new LocalJobPlanner(appDesc));
    runner = spy(new LocalApplicationRunner(appDesc, localPlanner));
  }

}
