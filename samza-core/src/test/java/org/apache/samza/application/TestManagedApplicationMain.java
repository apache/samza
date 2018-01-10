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
package org.apache.samza.application;

import java.io.File;
import java.io.IOException;
import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.apache.samza.runtime.AbstractApplicationRunner;
import org.apache.samza.runtime.ApplicationRuntimeResult;
import org.apache.samza.runtime.NoOpRuntimeResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestManagedApplicationMain {
  @Before
  public void setUp() {
    TestApplicationRunnerInvocationCounts.resetCounters();
    TestJobFactoryInvocationCounts.resetCounters();
  }

  @Test
  public void testRunOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.runCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", StreamApplication.AppConfig.APP_CLASS + "=org.apache.samza.application.TestManagedApplicationMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.application.TestManagedApplicationMain$TestApplicationRunnerInvocationCounts"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.runCount);
  }

  @Test
  public void testKillOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.killCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", StreamApplication.AppConfig.APP_CLASS + "=org.apache.samza.application.TestManagedApplicationMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.application.TestManagedApplicationMain$TestApplicationRunnerInvocationCounts",
        "--operation=kill"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.killCount);
  }

  @Test
  public void testStatusOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.statusCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", StreamApplication.AppConfig.APP_CLASS + "=org.apache.samza.application.TestManagedApplicationMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.application.TestManagedApplicationMain$TestApplicationRunnerInvocationCounts",
        "--operation=status"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.statusCount);
  }

  @Test
  public void testJobRunnerRunOperation() throws Exception {
    assertEquals(0, TestJobFactoryInvocationCounts.runCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", "job.factory.class=org.apache.samza.application.TestManagedApplicationMain$TestJobFactory",
    });

    assertEquals(1, TestJobFactoryInvocationCounts.runCount);
  }

  @Test
  public void testJobRunnerKillOperation() throws Exception {
    assertEquals(0, TestJobFactoryInvocationCounts.killCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", "job.factory.class=org.apache.samza.application.TestManagedApplicationMain$TestJobFactory",
        "--operation=kill"
    });

    assertEquals(1, TestJobFactoryInvocationCounts.killCount);
  }

  @Test
  public void testJobRunnerStatusOperation() throws Exception {
    assertEquals(0, TestJobFactoryInvocationCounts.statusCount);
    ManagedApplicationMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", "job.factory.class=org.apache.samza.application.TestManagedApplicationMain$TestJobFactory",
        "--operation=status"
    });

    assertEquals(1, TestJobFactoryInvocationCounts.statusCount);
  }

  public static class TestJobFactoryInvocationCounts implements StreamJob {
    protected static int runCount = 0;
    protected static int killCount = 0;
    protected static int statusCount = 0;

    public static void resetCounters() {
      runCount = 0;
      killCount = 0;
      statusCount = 0;
    }

    @Override
    public StreamJob submit() {
      runCount++;
      return this;
    }

    @Override
    public StreamJob kill() {
      killCount++;
      return this;
    }

    @Override
    public ApplicationStatus waitForFinish(long timeoutMs) {
      return ApplicationStatus.Running;
    }

    @Override
    public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {
      return ApplicationStatus.Running;
    }

    @Override
    public ApplicationStatus getStatus() {
      statusCount++;
      return ApplicationStatus.Running;
    }
  }

  public static class TestJobFactory implements StreamJobFactory {

    @Override
    public StreamJob getJob(Config config) {
      return new TestJobFactoryInvocationCounts();
    }
  }

  public static class TestApplicationRunnerInvocationCounts extends AbstractApplicationRunner {
    protected static int runCount = 0;
    protected static int killCount = 0;
    protected static int statusCount = 0;

    public static void resetCounters() {
      runCount = 0;
      killCount = 0;
      statusCount = 0;
    }

    public TestApplicationRunnerInvocationCounts(Config config) {
      super(config);
    }

    @Override
    public void runTask() {
      throw new UnsupportedOperationException("runTask() not supported in this test");
    }

    @Override
    public ApplicationRuntimeResult run(StreamApplication userApp) {
      TestApplicationRunnerInvocationCounts.this.runCount++;
      return new NoOpRuntimeResult();
    }

    @Override
    public ApplicationRuntimeResult kill(StreamApplication userApp) {
      TestApplicationRunnerInvocationCounts.this.killCount++;
      return new NoOpRuntimeResult();
    }

    @Override
    public ApplicationStatus status(StreamApplication userApp) {
      TestApplicationRunnerInvocationCounts.this.statusCount++;
      return ApplicationStatus.Running;
    }

  }

  public static class TestStreamApplicationDummy {

    private final StreamApplication app;

    public TestStreamApplicationDummy(Config config) throws IOException {
      this.app = StreamApplications.createStreamApp(config);
    }

    public static void main(String[] args) throws IOException {
      ManagedApplicationMain.ApplicationMainCommandLine cmd = new ManagedApplicationMain.ApplicationMainCommandLine();
      OptionSet options = cmd.parser().parse(args);
      Config config = cmd.loadConfig(options);
      TestStreamApplicationDummy dummyApp = new TestStreamApplicationDummy(config);
      ManagedApplicationMain.ApplicationMainOperation op = cmd.getOperation(options);
      ManagedApplicationMain.runCmd(dummyApp.app, op);
    }

  }

}
