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
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.ApplicationDescriptors;
import org.apache.samza.application.MockStreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestApplicationRunnerMain {

  @Test
  public void TestRunOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.runCount);
    ApplicationRunnerMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        getClass().getResource("/test.properties").getPath(),
        "-config", String.format("%s=%s", ApplicationConfig.APP_CLASS, MockStreamApplication.class.getName()),
        "-config", String.format("app.runner.class=%s", TestApplicationRunnerInvocationCounts.class.getName()),
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.runCount);
  }

  @Test
  public void TestKillOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.killCount);
    ApplicationRunnerMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        getClass().getResource("/test.properties").getPath(),
        "-config", String.format("%s=%s", ApplicationConfig.APP_CLASS, MockStreamApplication.class.getName()),
        "-config", String.format("app.runner.class=%s", TestApplicationRunnerInvocationCounts.class.getName()),
        "--operation=kill"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.killCount);
  }

  @Test
  public void TestStatusOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.statusCount);
    ApplicationRunnerMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        getClass().getResource("/test.properties").getPath(),
        "-config", String.format("%s=%s", ApplicationConfig.APP_CLASS, MockStreamApplication.class.getName()),
        "-config", String.format("app.runner.class=%s", TestApplicationRunnerInvocationCounts.class.getName()),
        "--operation=status"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.statusCount);
  }

  public static class TestApplicationRunnerInvocationCounts implements ApplicationRunner {
    protected static int runCount = 0;
    protected static int killCount = 0;
    protected static int statusCount = 0;

    public TestApplicationRunnerInvocationCounts(SamzaApplication userApp, Config config) {
    }

    @Override
    public void run() {
      runCount++;
    }

    @Override
    public void kill() {
      killCount++;
    }

    @Override
    public ApplicationStatus status() {
      statusCount++;
      return ApplicationStatus.Running;
    }

    @Override
    public void waitForFinish() {
      waitForFinish(Duration.ofSeconds(0));
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return false;
    }

  }
}
