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
import java.util.List;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.apache.samza.job.local.ProcessJobFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
    config.put(ApplicationConfig.APP_NAME, "test-app");
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
  public void testRunWithConfigLoaderFactoryPresent() {
    Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_NAME, "test-app");
    config.put(JobConfig.CONFIG_LOADER_FACTORY, "org.apache.samza.config.loaders.PropertiesConfigLoaderFactory");
    config.put(JobConfig.STREAM_JOB_FACTORY_CLASS, MockStreamJobFactory.class.getName());
    runner = new RemoteApplicationRunner(null, new MapConfig(config));

    runner.run(null);
  }

  @Test
  public void testGetStatus() {
    Map<String, String> m = new HashMap<>();
    m.put(JobConfig.JOB_NAME, "jobName");
    m.put(JobConfig.STREAM_JOB_FACTORY_CLASS, MockStreamJobFactory.class.getName());

    m.put(JobConfig.JOB_ID, "newJob");

    StreamApplication userApp = appDesc -> { };
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));

    Assert.assertEquals(ApplicationStatus.New, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));

    m.put(JobConfig.JOB_ID, "runningJob");
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));
    Assert.assertEquals(ApplicationStatus.Running, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));
  }

  @Test
  public void testLocalRunWithConfigLoaderFactoryPresent() {
    Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_NAME, "test-app");
    config.put(JobConfig.CONFIG_LOADER_FACTORY, "org.apache.samza.config.loaders.PropertiesConfigLoaderFactory");
    config.put(JobConfig.STREAM_JOB_FACTORY_CLASS, ProcessJobFactory.class.getName());

    try {
      runner.run(null);
      Assert.fail("Should have went to the planning phase");
    } catch (SamzaException e) {
      Assert.assertFalse(e.getMessage().contains("No jobs to run."));
    }
  }

  @Ignore //TODO: SAMZA-2738: Return real status for local jobs after avoiding recreating the Job in runner.status()
  @Test
  public void testLocalGetStatus() {
    Map<String, String> m = new HashMap<>();
    m.put(JobConfig.JOB_NAME, "jobName");
    m.put(JobConfig.STREAM_JOB_FACTORY_CLASS, ProcessJobFactory.class.getName());

    m.put(JobConfig.JOB_ID, "newJob");

    StreamApplication userApp = appDesc -> { };
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));

    Assert.assertEquals(ApplicationStatus.New, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));

    m.put(JobConfig.JOB_ID, "runningJob");
    runner = spy(new RemoteApplicationRunner(userApp, new MapConfig(m)));
    Assert.assertEquals(ApplicationStatus.Running, runner.getApplicationStatus(new JobConfig(new MapConfig(m))));
  }

  static public class MockRemoteJobPlanner extends RemoteJobPlanner {

    public MockRemoteJobPlanner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> descriptor) {
      super(descriptor);
    }

    @Override
    public List<JobConfig> prepareJobs() {
      return Collections.emptyList();
    }
  }

  static public class MockStreamJobFactory implements StreamJobFactory {

    public MockStreamJobFactory() {
    }

    @Override
    public StreamJob getJob(final Config config) {

      return new StreamJob() {
        JobConfig c = new JobConfig(config);

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
    }
  }
}
