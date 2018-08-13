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
import org.apache.samza.application.StreamAppDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.internal.AppDescriptorImpl;
import org.apache.samza.application.internal.StreamAppDescriptorImpl;
import org.apache.samza.application.internal.TaskAppDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.job.ApplicationStatus;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestApplicationRunnerMain {

  @Test
  public void TestRunOperation() throws Exception {
    assertEquals(0, TestApplicationRunnerInvocationCounts.runCount);
    ApplicationRunnerMain.main(new String[]{
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        getClass().getResource("/test.properties").getPath(),
        "-config", ApplicationConfig.APP_CLASS + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.runtime.TestApplicationRunnerMain$TestApplicationRunnerInvocationCounts"
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
        "-config", ApplicationConfig.APP_CLASS + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.runtime.TestApplicationRunnerMain$TestApplicationRunnerInvocationCounts",
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
        "-config", ApplicationConfig.APP_CLASS + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.runtime.TestApplicationRunnerMain$TestApplicationRunnerInvocationCounts",
        "--operation=status"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.statusCount);
  }

  public static class TestStreamApplicationDummy implements StreamApplication {

    @Override
    public void describe(StreamAppDescriptor appDesc) {

    }
  }

  public static class TestApplicationRunnerInvocationCounts extends AbstractApplicationRunner {
    protected static int runCount = 0;
    protected static int killCount = 0;
    protected static int statusCount = 0;

    public TestApplicationRunnerInvocationCounts(AppDescriptorImpl appDesc) {
      super(appDesc);
    }

    private void countRun() {
      runCount++;
    }

    private void countKill() {
      killCount++;
    }

    private ApplicationStatus countStatus() {
      statusCount++;
      return ApplicationStatus.Running;
    }

    class TestAppExecutable implements AppRuntimeExecutable {

      @Override
      public void run() {
        countRun();
      }

      @Override
      public void kill() {
        countKill();
      }

      @Override
      public ApplicationStatus status() {
        return countStatus();
      }

      @Override
      public boolean waitForFinish(Duration timeout) {
        return false;
      }
    }

    @Override
    AppRuntimeExecutable getTaskAppRuntimeExecutable(TaskAppDescriptorImpl appSpec) {
      return new TestAppExecutable();
    }

    @Override
    AppRuntimeExecutable getStreamAppRuntimeExecutable(StreamAppDescriptorImpl appSpec) {
      return new TestAppExecutable();
    }

  }

}
