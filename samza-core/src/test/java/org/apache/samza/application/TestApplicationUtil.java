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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.application.descriptors.TaskApplicationDescriptorImpl;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.task.MockStreamTask;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ApplicationUtil}
 */
public class TestApplicationUtil {

  @Test
  public void testStreamAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, MockStreamApplication.class.getName());
    SamzaApplication app = ApplicationUtil.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockStreamApplication);

    configMap.put(TaskConfig.TASK_CLASS, MockStreamTask.class.getName());
    app = ApplicationUtil.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockStreamApplication);
  }

  @Test
  public void testTaskAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, MockTaskApplication.class.getName());
    SamzaApplication app = ApplicationUtil.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockTaskApplication);

    configMap.put(TaskConfig.TASK_CLASS, MockStreamTask.class.getName());
    app = ApplicationUtil.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockTaskApplication);
  }

  @Test
  public void testTaskClassOnly() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TASK_CLASS, MockStreamTask.class.getName());
    Config config = new MapConfig(configMap);
    SamzaApplication app = ApplicationUtil.fromConfig(config);
    assertTrue(app instanceof TaskApplication);
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl((TaskApplication) app, config);
    assertTrue(appSpec.getTaskFactory().createInstance() instanceof MockStreamTask);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyTaskClassOnly() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TASK_CLASS, "");
    ApplicationUtil.fromConfig(new MapConfig(configMap));
  }

  @Test(expected = ConfigException.class)
  public void testNoAppClassNoTaskClass() {
    Map<String, String> configMap = new HashMap<>();
    ApplicationUtil.fromConfig(new MapConfig(configMap));
  }

  @Test
  public void testIsHighLevelJob() {
    final Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.HIGH_LEVEL.name());
    assertTrue(ApplicationUtil.isHighLevelApiJob(new MapConfig(configMap)));

    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LOW_LEVEL.name());
    assertFalse(ApplicationUtil.isHighLevelApiJob(new MapConfig(configMap)));

    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LEGACY.name());
    assertFalse(ApplicationUtil.isHighLevelApiJob(new MapConfig(configMap)));
  }

  /**
   * Test class of {@link TaskApplication} for unit tests
   */
  public static class MockTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {

    }
  }
}