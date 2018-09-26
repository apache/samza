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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


/**
 * A test class for {@link RemoteApplicationRunner}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RemoteApplicationRunner.class)
public class TestRemoteApplicationRunner {

  private RemoteApplicationRunner runner;

  @Before
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    StreamApplication userApp = appDesc -> { };
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(config)));
  }

  @Test
  public void testWaitForFinishReturnsBeforeTimeout() {
    doReturn(ApplicationStatus.SuccessfulFinish).when(runner).getApplicationStatus(any(JobConfig.class));
    boolean finished = runner.waitForFinish(Duration.ofMillis(5000));
    assertTrue("Application did not finish before the timeout.", finished);
  }

  @Test
  public void testWaitForFinishTimesout() {
    doReturn(ApplicationStatus.Running).when(runner).getApplicationStatus(any(JobConfig.class));
    boolean finished = runner.waitForFinish(Duration.ofMillis(1000));
    assertFalse("Application finished before the timeout.", finished);
  }

  @Test
  public void testGetStatus() {
    Map m = new HashMap<String, String>();
    m.put(JobConfig.JOB_NAME(), "jobName");
    m.put(JobConfig.STREAM_JOB_FACTORY_CLASS(), MockStreamJobFactory.class.getName());

    m.put(JobConfig.JOB_ID(), "newJob");

    StreamApplication userApp = appDesc -> { };
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));

    Assert.assertEquals(ApplicationStatus.New, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));

    m.put(JobConfig.JOB_ID(), "runningJob");
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));
    Assert.assertEquals(ApplicationStatus.Running, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));
  }

  static public class MockStreamJobFactory implements StreamJobFactory {

    public MockStreamJobFactory() {
    }

    @Override
    public StreamJob getJob(final Config config) {

      StreamJob streamJob = new StreamJob() {
        JobConfig c = (JobConfig) config;

        @Override
        public StreamJob submit() {
          return null;
        }

        @Override
        public StreamJob kill() {
          return null;
        }

        @Override
        public ApplicationStatus waitForFinish(long timeoutMs) {
          return null;
        }

        @Override
        public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {
          return null;
        }

        @Override
        public ApplicationStatus getStatus() {
          String jobId = c.getJobId();
          switch (jobId) {
            case "newJob":
              return ApplicationStatus.New;
            case "runningJob":
              return ApplicationStatus.Running;
            default:
              return ApplicationStatus.UnsuccessfulFinish;
          }
        }
      };

      return streamJob;
    }
  }
}
