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
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


/**
 * Unit test for {@link ApplicationRunners}
 */
public class TestApplicationRunners {

  @Test
  public void testGetAppRunner() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("app.runner.class", MockApplicationRunner.class.getName());
    Config config = new MapConfig(configMap);
    StreamApplication app = mock(StreamApplication.class);
    ApplicationRunner appRunner = ApplicationRunners.getApplicationRunner(app, config);
    assertTrue(appRunner instanceof MockApplicationRunner);
  }

  /**
   * Test class for {@link ApplicationRunners} unit test
   */
  public static class MockApplicationRunner implements ApplicationRunner {
    private final SamzaApplication userApp;
    private final Config config;

    public MockApplicationRunner(SamzaApplication userApp, Config config) {
      this.userApp = userApp;
      this.config = config;
    }

    @Override
    public void run() {

    }

    @Override
    public void kill() {

    }

    @Override
    public ApplicationStatus status() {
      return null;
    }

    @Override
    public void waitForFinish() {

    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return false;
    }

  }
}