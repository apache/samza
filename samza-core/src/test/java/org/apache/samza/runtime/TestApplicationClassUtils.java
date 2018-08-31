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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.TaskApplicationDescriptor;
import org.apache.samza.application.TaskApplicationDescriptorImpl;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.MockStreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.task.MockStreamTask;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * Unit tests for {@link ApplicationClassUtils}
 */
public class TestApplicationClassUtils {

  @Test
  public void testStreamAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, MockStreamApplication.class.getName());
    SamzaApplication app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockStreamApplication);

    configMap.put(TaskConfig.TASK_CLASS(), MockStreamTask.class.getName());
    app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockStreamApplication);
  }

  @Test
  public void testTaskAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, MockTaskApplication.class.getName());
    SamzaApplication app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockTaskApplication);

    configMap.put(TaskConfig.TASK_CLASS(), MockStreamTask.class.getName());
    app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof MockTaskApplication);
  }

  @Test
  public void testTaskClassOnly() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TASK_CLASS(), MockStreamTask.class.getName());
    Config config = new MapConfig(configMap);
    SamzaApplication app = ApplicationClassUtils.fromConfig(config);
    assertTrue(app instanceof TaskApplication);
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl((TaskApplication) app, config);
    assertTrue(appSpec.getTaskFactory().createInstance() instanceof MockStreamTask);
  }

  @Test(expected = ConfigException.class)
  public void testNoAppClassNoTaskClass() {
    Map<String, String> configMap = new HashMap<>();
    ApplicationClassUtils.fromConfig(new MapConfig(configMap));
  }

  /**
   * Test class of {@link TaskApplication} for unit tests
   */
  public static class MockTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appSpec) {

    }
  }
}