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

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.task.IdentityStreamTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
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
  public void testRunStreamTask() {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(ApplicationConfig.APP_NAME, "test-app");
    cfgs.put(ApplicationConfig.APP_ID, "test-appId");
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

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)));
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).status();

    runner.run(externalContext);

    assertEquals(ApplicationStatus.SuccessfulFinish, runner.status());
  }

  @Test
  public void testRunStreamTaskWithoutExternalContext() {
    final Map<String, String> cfgs = new HashMap<>();
    cfgs.put(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
    cfgs.put(ApplicationConfig.APP_NAME, "test-app");
    cfgs.put(ApplicationConfig.APP_ID, "test-appId");
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

    doReturn(sp).when(runner).createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.empty()));
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).status();

    runner.run();

    assertEquals(ApplicationStatus.SuccessfulFinish, runner.status());
  }

  @Test
  public void testRunComplete() {
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

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)));

    runner.run(externalContext);
    runner.waitForFinish();

    assertEquals(runner.status(), ApplicationStatus.SuccessfulFinish);
  }

  @Test
  public void testRunFailure() {
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

    ExternalContext externalContext = mock(ExternalContext.class);
    doReturn(sp).when(runner)
        .createStreamProcessor(anyObject(), anyObject(), captor.capture(), eq(Optional.of(externalContext)));

    try {
      runner.run(externalContext);
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

  @Test
  public void testCreateProcessorIdShouldReturnProcessorIdDefinedInConfiguration() {
    String processorId = "testProcessorId";
    MapConfig configMap = new MapConfig(ImmutableMap.of(ApplicationConfig.PROCESSOR_ID, processorId));
    String actualProcessorId = LocalApplicationRunner.createProcessorId(new ApplicationConfig(configMap));
    assertEquals(processorId, actualProcessorId);
  }

  @Test
  public void testCreateProcessorIdShouldInvokeProcessorIdGeneratorDefinedInConfiguration() {
    String processorId = "testProcessorId";
    MapConfig configMap = new MapConfig(ImmutableMap.of(ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS, MockProcessorIdGenerator.class.getCanonicalName()));
    String actualProcessorId = LocalApplicationRunner.createProcessorId(new ApplicationConfig(configMap));
    assertEquals(processorId, actualProcessorId);
  }

  @Test(expected = ConfigException.class)
  public void testCreateProcessorIdShouldThrowExceptionWhenProcessorIdAndGeneratorAreNotDefined() {
    ApplicationConfig mockConfig = Mockito.mock(ApplicationConfig.class);
    Mockito.when(mockConfig.getProcessorId()).thenReturn(null);
    LocalApplicationRunner.createProcessorId(mockConfig);
  }

  private void prepareTest() {
    ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
        ApplicationDescriptorUtil.getAppDescriptor(mockApp, config);
    localPlanner = spy(new LocalJobPlanner(appDesc));
    runner = spy(new LocalApplicationRunner(appDesc, localPlanner));
  }

}
