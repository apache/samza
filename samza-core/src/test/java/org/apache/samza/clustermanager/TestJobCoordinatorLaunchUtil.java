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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.MockStreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.coordinator.StaticResourceJobCoordinator;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.MetricsReporterLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    StaticResourceJobCoordinator.class,
    MetricsReporterLoader.class})
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
  public void testRunStaticResourceJobCoordinator() throws Exception {
    Config originalConfig =
        buildOriginalConfig(ImmutableMap.of(JobCoordinatorConfig.USE_STATIC_RESOURCE_JOB_COORDINATOR, "true"));
    JobConfig fullConfig =
        new JobConfig(new MapConfig(originalConfig, Collections.singletonMap("isAfterPlanning", "true")));
    Config autoSizingConfig = new MapConfig(Collections.singletonMap(JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT, "10"));
    Config finalConfig = new MapConfig(autoSizingConfig, fullConfig);

    RemoteJobPlanner remoteJobPlanner = mock(RemoteJobPlanner.class);
    CoordinatorStreamStore coordinatorStreamStore = mock(CoordinatorStreamStore.class);
    StaticResourceJobCoordinator staticResourceJobCoordinator = mock(StaticResourceJobCoordinator.class);

    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    PowerMockito.doNothing().when(CoordinatorStreamUtil.class, "createCoordinatorStream", any());
    PowerMockito.doReturn(new MapConfig()).when(CoordinatorStreamUtil.class, "buildCoordinatorStreamConfig", any());
    PowerMockito.doReturn(autoSizingConfig)
        .when(CoordinatorStreamUtil.class, "readLaunchConfigFromCoordinatorStream", any(), any());
    PowerMockito.whenNew(CoordinatorStreamStore.class).withAnyArguments().thenReturn(coordinatorStreamStore);
    PowerMockito.whenNew(RemoteJobPlanner.class).withAnyArguments().thenReturn(remoteJobPlanner);
    when(remoteJobPlanner.prepareJobs()).thenReturn(Collections.singletonList(fullConfig));
    PowerMockito.mockStatic(StaticResourceJobCoordinator.class);
    PowerMockito.doReturn(staticResourceJobCoordinator)
        .when(StaticResourceJobCoordinator.class, "build", any(), eq(coordinatorStreamStore), eq(finalConfig));
    PowerMockito.spy(JobCoordinatorLaunchUtil.class);
    PowerMockito.doNothing().when(JobCoordinatorLaunchUtil.class, "addShutdownHook", any());
    MetricsReporter metricsReporter = mock(MetricsReporter.class);
    Map<String, MetricsReporter> metricsReporterMap = ImmutableMap.of("reporter", metricsReporter);
    PowerMockito.mockStatic(MetricsReporterLoader.class);
    PowerMockito.doReturn(metricsReporterMap)
        .when(MetricsReporterLoader.class, "getMetricsReporters", new MetricsConfig(finalConfig), "JobCoordinator");

    JobCoordinatorLaunchUtil.run(new MockStreamApplication(), originalConfig);

    verifyStatic();
    CoordinatorStreamUtil.createCoordinatorStream(fullConfig);
    verifyStatic();
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(finalConfig, true);
    verifyStatic();
    JobCoordinatorLaunchUtil.addShutdownHook(staticResourceJobCoordinator);
    InOrder inOrder = Mockito.inOrder(metricsReporter, staticResourceJobCoordinator);
    inOrder.verify(metricsReporter).register(eq("JobCoordinator"), any());
    inOrder.verify(metricsReporter).start();
    inOrder.verify(staticResourceJobCoordinator).run();
    inOrder.verify(metricsReporter).stop();
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
