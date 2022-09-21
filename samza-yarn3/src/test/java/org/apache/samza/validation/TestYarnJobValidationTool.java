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

package org.apache.samza.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.metrics.JmxMetricsAccessor;
import org.apache.samza.metrics.MetricsValidationFailureException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class TestYarnJobValidationTool {
  private YarnClient client;
  private YarnJobValidationTool tool;
  private String jobName = "test";
  private int jobId = 1;
  private ApplicationId appId;
  ApplicationAttemptId attemptId;
  private int containerCount = 9;
  private Config config = new MapConfig(new HashMap<String, String>() {
    {
      put("job.name", jobName);
      put("job.id", String.valueOf(jobId));
      put("yarn.container.count", String.valueOf(containerCount));
    }
  });
  private MockMetricsValidator validator = new MockMetricsValidator();

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    client = mock(YarnClient.class);
    tool = new YarnJobValidationTool(new JobConfig(config), client, validator);
    appId = mock(ApplicationId.class);
    when(appId.getId()).thenReturn(1111);
    attemptId = mock(ApplicationAttemptId.class);
    when(attemptId.getApplicationId()).thenReturn(appId);
    when(attemptId.getAttemptId()).thenReturn(2222);
  }

  @Test
  public void testValidateAppId() throws Exception {
    ApplicationReport appReport = mock(ApplicationReport.class);
    when(appReport.getName()).thenReturn(jobName + "_" + jobId);
    when(appReport.getApplicationId()).thenReturn(appId);
    when(client.getApplications()).thenReturn(Collections.singletonList(appReport));
    assertTrue(tool.validateAppId().equals(appId));

    when(appReport.getName()).thenReturn("dummy");
    exception.expect(SamzaException.class);
    tool.validateAppId();
  }

  @Test
  public void testValidateRunningAttemptId() throws Exception {
    ApplicationReport appReport = mock(ApplicationReport.class);
    when(client.getApplicationReport(appId)).thenReturn(appReport);
    when(appReport.getCurrentApplicationAttemptId()).thenReturn(attemptId);
    ApplicationAttemptReport attemptReport = mock(ApplicationAttemptReport.class);
    when(attemptReport.getYarnApplicationAttemptState()).thenReturn(YarnApplicationAttemptState.RUNNING);
    when(attemptReport.getApplicationAttemptId()).thenReturn(attemptId);
    when(client.getApplicationAttemptReport(attemptId)).thenReturn(attemptReport);
    assertTrue(tool.validateRunningAttemptId(appId).equals(attemptId));

    when(attemptReport.getYarnApplicationAttemptState()).thenReturn(YarnApplicationAttemptState.FAILED);
    exception.expect(SamzaException.class);
    tool.validateRunningAttemptId(appId);
  }

  @Test
  public void testValidateContainerCount() throws Exception {
    List<ContainerReport> containerReports = new ArrayList<>();
    for (int i = 0; i <= containerCount; i++) {
      ContainerReport report = mock(ContainerReport.class);
      when(report.getContainerState()).thenReturn(ContainerState.RUNNING);
      containerReports.add(report);
    }
    when(client.getContainers(attemptId)).thenReturn(containerReports);
    assertTrue(tool.validateContainerCount(attemptId) == (containerCount + 1));

    containerReports.remove(0);
    exception.expect(SamzaException.class);
    tool.validateContainerCount(attemptId);
  }

  @Test
  public void testValidateJmxMetrics() throws MetricsValidationFailureException {
    JmxMetricsAccessor jmxMetricsAccessor = mock(JmxMetricsAccessor.class);
    Map<String, Long> values = new HashMap<>();
    values.put("samza-container-0", 100L);
    when(jmxMetricsAccessor.getCounterValues(SamzaContainerMetrics.class.getName(), "commit-calls")).thenReturn(values);
    validator.validate(jmxMetricsAccessor);

    values.put("samza-container-0", -1L);
    // the mock validator will fail if the commit-calls are less than or equal to 0
    exception.expect(MetricsValidationFailureException.class);
    validator.validate(jmxMetricsAccessor);
  }
}
