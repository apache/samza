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
package org.apache.samza.task;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.testUtils.TestAsyncStreamTask;
import org.apache.samza.testUtils.TestStreamTask;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Test methods to create {@link StreamTaskFactory} or {@link AsyncStreamTaskFactory} based on task class configuration
 */
public class TestTaskFactoryUtil {

  private final ApplicationRunner mockRunner = mock(ApplicationRunner.class);

  @Test
  public void testStreamTaskClass() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
      }
    });
    TaskFactory retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testStreamOperatorTaskClass() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    Object retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ empty class name for StreamApplication");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<>());
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail(String.format("Should have failed w/ non-existing entry for %s", StreamApplication.APP_CLASS_CONFIG));
    } catch (ConfigException ce) {
      // expected
    }
  }

  @Test
  public void testStreamOperatorTaskClassWithTaskClass() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    TaskFactory retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("should have failed with invalid config");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("should have failed with invalid config");
    } catch (ConfigException ce) {
      // expected
    }
  }

  @Test
  public void testStreamTaskClassWithInvalidStreamGraphBuilder() {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("should have failed with invalid config");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
        this.put(StreamApplication.APP_CLASS_CONFIG, "");
      }
    });
    TaskFactory retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "");
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no class not found");
    } catch (ConfigException cne) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTask() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
      }
    });
    TaskFactory retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTaskWithInvalidStreamGraphBuilder() throws ClassNotFoundException {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamApplication.APP_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamApplication.APP_CLASS_CONFIG, "");
      }
    });
    TaskFactory retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamApplication.APP_CLASS_CONFIG, null);
      }
    });
    retFactory = TaskFactoryUtil.fromTaskClassConfig(config, mockRunner);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);
  }

  @Test
  public void testFinalizeTaskFactory() throws NoSuchFieldException, IllegalAccessException {
    TaskFactory<Object> mockFactory = mock(TaskFactory.class);
    try {
      TaskFactoryUtil.finalizeTaskFactory(mockFactory, true, null);
      fail("Should have failed with validation");
    } catch (SamzaException se) {
      // expected
    }
    StreamTaskFactory mockStreamFactory = mock(StreamTaskFactory.class);
    TaskFactory retFactory = TaskFactoryUtil.finalizeTaskFactory(mockStreamFactory, true, null);
    assertEquals(retFactory, mockStreamFactory);

    ExecutorService mockThreadPool = mock(ExecutorService.class);
    retFactory = TaskFactoryUtil.finalizeTaskFactory(mockStreamFactory, false, mockThreadPool);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(retFactory.createInstance() instanceof AsyncStreamTaskAdapter);
    AsyncStreamTaskAdapter taskAdapter = (AsyncStreamTaskAdapter) retFactory.createInstance();
    Field executorSrvFld = AsyncStreamTaskAdapter.class.getDeclaredField("executor");
    executorSrvFld.setAccessible(true);
    ExecutorService executor = (ExecutorService) executorSrvFld.get(taskAdapter);
    assertEquals(executor, mockThreadPool);

    AsyncStreamTaskFactory mockAsyncStreamFactory = mock(AsyncStreamTaskFactory.class);
    try {
      TaskFactoryUtil.finalizeTaskFactory(mockAsyncStreamFactory, true, null);
      fail("Should have failed");
    } catch (SamzaException se) {
      // expected
    }

    retFactory = TaskFactoryUtil.finalizeTaskFactory(mockAsyncStreamFactory, false, null);
    assertEquals(retFactory, mockAsyncStreamFactory);
  }
}
