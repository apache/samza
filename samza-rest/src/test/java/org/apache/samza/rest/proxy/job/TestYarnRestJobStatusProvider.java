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
package org.apache.samza.rest.proxy.job;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import junit.framework.TestCase;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestYarnRestJobStatusProvider extends TestCase {
  private YarnRestJobStatusProvider provider;

  private static final String APPS_RESPONSE =
      "{\"apps\":{\"app\":[{\"id\":\"application_1502919535296_0161\",\"name\":\"job1_1\",\"state\":\"KILLED\",\"finalStatus\":\"KILLED\",\"applicationType\":\"Samza\"},"
          + "{\"id\":\"application_1502919535296_0163\",\"name\":\"job1_1\",\"state\":\"RUNNING\",\"finalStatus\":\"UNDEFINED\",\"applicationType\":\"Samza\"},"
          + "{\"id\":\"application_1502919535296_0162\",\"name\":\"job1_1\",\"state\":\"KILLED\",\"finalStatus\":\"KILLED\",\"applicationType\":\"Samza\"},"
          + "{\"id\":\"application_1502919535296_0165\",\"name\":\"job2_1\",\"state\":\"KILLED\",\"finalStatus\":\"KILLED\",\"applicationType\":\"Samza\"},"
          + "{\"id\":\"application_1502919535296_0164\",\"name\":\"job3_1\",\"state\":\"RUNNING\",\"finalStatus\":\"UNDEFINED\",\"applicationType\":\"Samza\"}]}}";

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    provider = spy(new YarnRestJobStatusProvider(new JobsResourceConfig(new MapConfig())));
  }

  @Test
  public void testGetJobStatuses() throws IOException, InterruptedException {
    doReturn(APPS_RESPONSE.getBytes()).when(provider).httpGet(anyString());

    List<Job> jobs = Lists.newArrayList(
        new Job("job1", "1"),  // Job with multiple applications, 1 RUNNING
        new Job("job2", "1"),  // Job with 1 KILLED application
        new Job("job3", "1"),  // Job with 1 RUNNING application
        new Job("job4", "1")); // Job not found in YARN
    provider.getJobStatuses(jobs);

    Collections.sort(jobs, (o1, o2) -> o1.getJobName().compareTo(o2.getJobName()));

    assertEquals(4, jobs.size());
    verifyJobStatus(jobs.get(0), "job1", JobStatus.STARTED, "RUNNING");
    verifyJobStatus(jobs.get(1), "job2", JobStatus.STOPPED, "KILLED");
    verifyJobStatus(jobs.get(2), "job3", JobStatus.STARTED, "RUNNING");
    verifyJobStatus(jobs.get(3), "job4", JobStatus.UNKNOWN, null);
  }

  private void verifyJobStatus(Job job, String jobName, JobStatus samzaStatus, String yarnStatus) {
    assertEquals(jobName, job.getJobName());
    assertEquals(samzaStatus, job.getStatus());
    assertEquals(yarnStatus, job.getStatusDetail());
  }
}
