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

import java.io.File;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInternal;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.StreamSpec;
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
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", ApplicationRunnerMain.STREAM_APPLICATION_CLASS_CONFIG + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
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
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", ApplicationRunnerMain.STREAM_APPLICATION_CLASS_CONFIG + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
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
        String.format("file://%s/src/test/resources/test.properties", new File(".").getCanonicalPath()),
        "-config", ApplicationRunnerMain.STREAM_APPLICATION_CLASS_CONFIG + "=org.apache.samza.runtime.TestApplicationRunnerMain$TestStreamApplicationDummy",
        "-config", "app.runner.class=org.apache.samza.runtime.TestApplicationRunnerMain$TestApplicationRunnerInvocationCounts",
        "--operation=status"
    });

    assertEquals(1, TestApplicationRunnerInvocationCounts.statusCount);
  }

  public static class TestApplicationRunnerInvocationCounts extends ApplicationRunnerBase {
    protected static int runCount = 0;
    protected static int killCount = 0;
    protected static int statusCount = 0;

    public TestApplicationRunnerInvocationCounts(Config config) {
      super(config);
    }

    @Override
    ApplicationRuntimeInstance createRuntimeInstance(ApplicationBase app) {
      if (app instanceof StreamApplication) {
        return new StreamApplicationRuntime((StreamApplication) app);
      }
      throw new UnsupportedOperationException("Application class not supported");
    }

    @Override
    public void runTask() {
      throw new UnsupportedOperationException("runTask() not supported in this test");
    }

    @Override
    public StreamSpec getStreamSpec(String streamId) {
      return null;
    }

    private class StreamApplicationRuntime implements ApplicationRuntimeInstance {
      public StreamApplicationRuntime(StreamApplication app) {
      }

      @Override
      public void run() {
        TestApplicationRunnerInvocationCounts.this.runCount++;
      }

      @Override
      public void kill() {
        TestApplicationRunnerInvocationCounts.this.killCount++;
      }

      @Override
      public ApplicationStatus status() {
        TestApplicationRunnerInvocationCounts.this.statusCount++;
        return ApplicationStatus.Running;
      }

      @Override
      public void waitForFinish() {

      }

      @Override
      public ApplicationBase getUserApp() {
        return null;
      }
    }
  }

}
