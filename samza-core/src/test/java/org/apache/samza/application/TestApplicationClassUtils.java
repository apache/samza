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
import org.apache.samza.application.internal.TaskAppDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.task.TestStreamTask;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ApplicationClassUtils}
 */
public class TestApplicationClassUtils {

  @Test
  public void testStreamAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, TestStreamApplication.class.getName());
    ApplicationBase app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof TestStreamApplication);

    configMap.put(TaskConfig.TASK_CLASS(), TestStreamTask.class.getName());
    app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof TestStreamApplication);
  }

  @Test
  public void testTaskAppClass() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_CLASS, TestTaskApplication.class.getName());
    ApplicationBase app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof TestTaskApplication);

    configMap.put(TaskConfig.TASK_CLASS(), TestStreamTask.class.getName());
    app = ApplicationClassUtils.fromConfig(new MapConfig(configMap));
    assertTrue(app instanceof TestTaskApplication);
  }

  @Test
  public void testTaskClassOnly() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(TaskConfig.TASK_CLASS(), TestStreamTask.class.getName());
    Config config = new MapConfig(configMap);
    ApplicationBase app = ApplicationClassUtils.fromConfig(config);
    assertTrue(app instanceof TaskApplication);
    TaskAppDescriptorImpl appSpec = new TaskAppDescriptorImpl((TaskApplication) app, config);
    assertTrue(appSpec.getTaskFactory().createInstance() instanceof TestStreamTask);
  }

  @Test(expected = ConfigException.class)
  public void testNoAppClassNoTaskClass() {
    Map<String, String> configMap = new HashMap<>();
    ApplicationClassUtils.fromConfig(new MapConfig(configMap));
  }
}
