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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtil.class})
public class TestDiagnosticsUtil {
  private static final String STREAM_NAME = "someStreamName";
  private static final String JOB_NAME = "someJob";
  private static final String JOB_ID = "someId";
  private static final String CONTAINER_ID = "someContainerId";
  private static final String ENV_ID = "someEnvID";
  private static final String SAMZA_EPOCH_ID = "someEpochID";
  public static final String REPORTER_FACTORY = "org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory";
  public static final String SYSTEM_FACTORY = "com.foo.system.SomeSystemFactory";

  @Test
  public void testBuildDiagnosticsManager() {
    Config config = new MapConfig(buildTestConfigs());
    JobModel mockJobModel = mock(JobModel.class);
    SystemFactory systemFactory = mock(SystemFactory.class);
    SystemProducer mockProducer = mock(SystemProducer.class);
    when(systemFactory.getProducer(anyString(), any(Config.class), any(MetricsRegistry.class), anyString())).thenReturn(mockProducer);
    PowerMockito.mockStatic(ReflectionUtil.class);
    when(ReflectionUtil.getObj(SYSTEM_FACTORY, SystemFactory.class)).thenReturn(systemFactory);

    Optional<DiagnosticsManager> diagnosticsManager =
        DiagnosticsUtil.buildDiagnosticsManager(JOB_NAME, JOB_ID, mockJobModel, CONTAINER_ID, Optional.of(ENV_ID),
            Optional.of(SAMZA_EPOCH_ID), config);

    Assert.assertTrue(diagnosticsManager.isPresent());
  }

  private Map<String, String> buildTestConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_DIAGNOSTICS_ENABLED, "true");
    configs.put(String.format(MetricsConfig.METRICS_REPORTER_FACTORY,
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS), REPORTER_FACTORY);
    configs.put(String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM,
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS),
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS + "." + STREAM_NAME);
    configs.put(String.format(SystemConfig.SYSTEM_FACTORY_FORMAT, MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS),
        SYSTEM_FACTORY);

    return configs;
  }
}
