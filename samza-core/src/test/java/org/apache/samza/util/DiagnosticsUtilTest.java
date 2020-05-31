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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.diagnostics.DiagnosticsManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
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
public class DiagnosticsUtilTest {

  private static final String STREAM_NAME = "someStreamName";
  private static final String JOB_NAME = "someJob";
  private static final String JOB_ID = "someId";
  private static final String CONTAINER_ID = "someContainerId";
  private static final String ENV_ID = "someEnvID";
  public static final String REPORTER_FACTORY = "org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory";

  @Test
  public void testBuildDiagnosticsManagerUsesConfiguredReporter() {
    Config config = new MapConfig(buildTestConfigs());
    JobModel mockJobModel = mock(JobModel.class);
    SystemProducer mockProducer = mock(SystemProducer.class);
    MetricsReporterFactory metricsReporterFactory = mock(MetricsReporterFactory.class);
    MetricsSnapshotReporter mockReporter = mock(MetricsSnapshotReporter.class);

    when(metricsReporterFactory.getMetricsReporter(anyString(), anyString(), any(Config.class))).thenReturn(
        mockReporter);
    PowerMockito.mockStatic(ReflectionUtil.class);
    when(ReflectionUtil.getObj(REPORTER_FACTORY, MetricsReporterFactory.class)).thenReturn(metricsReporterFactory);
    when(mockReporter.getProducer()).thenReturn(mockProducer);

    Optional<Pair<DiagnosticsManager, MetricsSnapshotReporter>> managerReporterPair =
        DiagnosticsUtil.buildDiagnosticsManager(JOB_NAME, JOB_ID, mockJobModel, CONTAINER_ID, Optional.of(ENV_ID),
            config);

    Assert.assertTrue(managerReporterPair.isPresent());
    Assert.assertEquals(mockReporter, managerReporterPair.get().getValue());
  }

  private Map<String, String> buildTestConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_DIAGNOSTICS_ENABLED, "true");
    configs.put(String.format(MetricsConfig.METRICS_REPORTER_FACTORY,
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS), REPORTER_FACTORY);
    configs.put(String.format(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM,
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS),
        MetricsConfig.METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS + "." + STREAM_NAME);

    return configs;
  }
}
