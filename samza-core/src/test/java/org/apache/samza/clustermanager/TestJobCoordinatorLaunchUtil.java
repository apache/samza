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
package org.apache.samza.clustermanager;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.MockStreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.coordinator.NoProcessorJobCoordinatorListener;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ReflectionUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;


@RunWith(PowerMockRunner.class)
@PrepareForTest({CoordinatorStreamUtil.class,
    JobCoordinatorLaunchUtil.class,
    CoordinatorStreamStore.class,
    RemoteJobPlanner.class,
    ReflectionUtil.class,
    MetricsReporterLoader.class,
    NoProcessorJobCoordinatorListener.class})
public class TestJobCoordinatorLaunchUtil {
  @Test
  public void testRunClusterBasedJobCoordinator() throws Exception {
    Config originalConfig = buildOriginalConfig(ImmutableMap.of());
    JobConfig fullConfig =
        new JobConfig(new MapConfig(originalConfig, Collections.singletonMap("isAfterPlanning", "true")));
    Config autoSizingConfig = new MapConfig(Collections.singletonMap(JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT, "10"));
    Config finalConfig = new MapConfig(autoSizingConfig, fullConfig);

    RemoteJobPlanner mockJobPlanner = mock(RemoteJobPlanner.class);
    CoordinatorStreamStore mockCoordinatorStreamStore = mock(CoordinatorStreamStore.class);
    ClusterBasedJobCoordinator mockJC = mock(ClusterBasedJobCoordinator.class);

    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    PowerMockito.doNothing().when(CoordinatorStreamUtil.class, "createCoordinatorStream", any());
    PowerMockito.doReturn(new MapConfig()).when(CoordinatorStreamUtil.class, "buildCoordinatorStreamConfig", any());
    PowerMockito.doReturn(autoSizingConfig).when(CoordinatorStreamUtil.class, "readLaunchConfigFromCoordinatorStream", any(), any());
    PowerMockito.whenNew(CoordinatorStreamStore.class).withAnyArguments().thenReturn(mockCoordinatorStreamStore);
    PowerMockito.whenNew(RemoteJobPlanner.class).withAnyArguments().thenReturn(mockJobPlanner);
    PowerMockito.whenNew(ClusterBasedJobCoordinator.class).withAnyArguments().thenReturn(mockJC);
    when(mockJobPlanner.prepareJobs()).thenReturn(Collections.singletonList(fullConfig));

    JobCoordinatorLaunchUtil.run(new MockStreamApplication(), originalConfig);

    verifyNew(ClusterBasedJobCoordinator.class).withArguments(any(MetricsRegistryMap.class), eq(mockCoordinatorStreamStore), eq(finalConfig));
    verify(mockJC, times(1)).run();
    verifyStatic(times(1));
    CoordinatorStreamUtil.createCoordinatorStream(fullConfig);
    verifyStatic(times(1));
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(finalConfig, true);
  }

  @Test
  public void testRunJobCoordinator() throws Exception {
    String jobCoordinatorFactoryClass = "org.apache.samza.custom.MyJobCoordinatorFactory";
    Config originalConfig =
        buildOriginalConfig(ImmutableMap.of(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, jobCoordinatorFactoryClass));
    JobConfig fullConfig =
        new JobConfig(new MapConfig(originalConfig, Collections.singletonMap("isAfterPlanning", "true")));
    Config autoSizingConfig = new MapConfig(Collections.singletonMap(JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT, "10"));
    Config finalConfig = new MapConfig(autoSizingConfig, fullConfig);

    RemoteJobPlanner remoteJobPlanner = mock(RemoteJobPlanner.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    JobCoordinatorFactory jobCoordinatorFactory = mock(JobCoordinatorFactory.class);
    JobCoordinator jobCoordinator = mock(JobCoordinator.class);
    // use a latch to keep track of when start has been called
    CountDownLatch jobCoordinatorStartedLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      jobCoordinatorStartedLatch.countDown();
      return null;
    }).when(jobCoordinator).start();

    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    PowerMockito.doNothing().when(CoordinatorStreamUtil.class, "createCoordinatorStream", any());
    PowerMockito.doReturn(new MapConfig()).when(CoordinatorStreamUtil.class, "buildCoordinatorStreamConfig", any());
    PowerMockito.doReturn(autoSizingConfig)
        .when(CoordinatorStreamUtil.class, "readLaunchConfigFromCoordinatorStream", any(), any());
    PowerMockito.whenNew(CoordinatorStreamStore.class).withAnyArguments().thenReturn(coordinatorStreamStore);
    PowerMockito.whenNew(RemoteJobPlanner.class).withAnyArguments().thenReturn(remoteJobPlanner);
    when(remoteJobPlanner.prepareJobs()).thenReturn(Collections.singletonList(fullConfig));
    PowerMockito.mockStatic(ReflectionUtil.class);
    PowerMockito.doReturn(jobCoordinatorFactory)
        .when(ReflectionUtil.class, "getObj", jobCoordinatorFactoryClass, JobCoordinatorFactory.class);
    when(jobCoordinatorFactory.getJobCoordinator(eq("samza-job-coordinator"), eq(finalConfig), any(),
        eq(coordinatorStreamStore))).thenReturn(jobCoordinator);
    PowerMockito.spy(JobCoordinatorLaunchUtil.class);
    PowerMockito.doNothing().when(JobCoordinatorLaunchUtil.class, "addShutdownHook", any());
    MetricsReporter metricsReporter = mock(MetricsReporter.class);
    Map<String, MetricsReporter> metricsReporterMap = ImmutableMap.of("reporter", metricsReporter);
    PowerMockito.mockStatic(MetricsReporterLoader.class);
    PowerMockito.doReturn(metricsReporterMap)
        .when(MetricsReporterLoader.class, "getMetricsReporters", new MetricsConfig(finalConfig), "JobCoordinator");
    NoProcessorJobCoordinatorListener jobCoordinatorListener = mock(NoProcessorJobCoordinatorListener.class);
    PowerMockito.whenNew(NoProcessorJobCoordinatorListener.class).withAnyArguments().thenReturn(jobCoordinatorListener);

    Thread runThread = new Thread(() -> JobCoordinatorLaunchUtil.run(new MockStreamApplication(), originalConfig));
    runThread.start();
    // wait for job coordinator to be started before doing verifications
    jobCoordinatorStartedLatch.await();

    verifyStatic();
    CoordinatorStreamUtil.createCoordinatorStream(fullConfig);
    verifyStatic();
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(finalConfig, true);
    verifyStatic();
    JobCoordinatorLaunchUtil.addShutdownHook(jobCoordinator);
    InOrder inOrder = Mockito.inOrder(metricsReporter, jobCoordinator);
    inOrder.verify(metricsReporter).register(eq("JobCoordinator"), any());
    inOrder.verify(metricsReporter).start();
    ArgumentCaptor<CountDownLatch> countDownLatchArgumentCaptor = ArgumentCaptor.forClass(CountDownLatch.class);
    verifyNew(NoProcessorJobCoordinatorListener.class).withArguments(countDownLatchArgumentCaptor.capture());
    inOrder.verify(jobCoordinator).setListener(jobCoordinatorListener);
    inOrder.verify(jobCoordinator).start();

    // wait some time and then make sure the run thread is still alive
    Thread.sleep(Duration.ofMillis(500).toMillis());
    assertTrue(runThread.isAlive());

    // trigger the count down latch so that the run thread can exit
    countDownLatchArgumentCaptor.getValue().countDown();
    runThread.join(Duration.ofSeconds(10).toMillis());
    assertFalse(runThread.isAlive());

    verify(metricsReporter).stop();
  }

  private static Config buildOriginalConfig(Map<String, String> additionalConfig) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("job.factory.class", "org.apache.samza.job.MockJobFactory");
    configMap.put("job.name", "test-job");
    configMap.put("foo", "bar");
    configMap.put("systems.coordinator.samza.factory",
        "org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory");
    configMap.put("job.coordinator.system", "coordinator");
    configMap.putAll(additionalConfig);
    return new MapConfig(configMap);
  }
}
