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
import org.apache.samza.application.MockStreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    CoordinatorStreamUtil.class,
    JobCoordinatorLaunchUtil.class,
    CoordinatorStreamStore.class,
    RemoteJobPlanner.class})
public class TestJobCoordinatorLaunchUtil {
  @Test
  public void testCreateFromConfigLoader() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(JobConfig.CONFIG_LOADER_FACTORY, PropertiesConfigLoaderFactory.class.getName());
    config.put(PropertiesConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + "path",
        getClass().getResource("/test.properties").getPath());
    Config originalConfig = new JobConfig(ConfigUtil.loadConfig(new MapConfig(config)));
    Config fullConfig = new MapConfig(originalConfig, Collections.singletonMap("isAfterPlanning", "true"));
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
    when(mockJobPlanner.prepareJobs()).thenReturn(Collections.singletonList(new JobConfig(fullConfig)));

    JobCoordinatorLaunchUtil.run(new MockStreamApplication(), originalConfig);

    verifyNew(ClusterBasedJobCoordinator.class).withArguments(any(MetricsRegistryMap.class), eq(mockCoordinatorStreamStore), eq(finalConfig));
    verify(mockJC, times(1)).run();
    verifyStatic(times(1));
    CoordinatorStreamUtil.createCoordinatorStream(any());
    verifyStatic(times(1));
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(any(), anyBoolean());
  }
}
