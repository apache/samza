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

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.impl.OperatorSpecGraph;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.testUtils.TestAsyncStreamTask;
import org.apache.samza.testUtils.TestStreamTask;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    Object retFactory = TaskFactoryUtil.createTaskFactory(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.createTaskFactory(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testCreateStreamApplication() throws Exception {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    StreamApplication streamApp = TaskFactoryUtil.createStreamApplication(config);
    assertNotNull(streamApp);
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, config);
    streamApp.init(graph, config);
    Object retFactory = TaskFactoryUtil.createTaskFactory(new OperatorSpecGraph(graph), null);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "");
      }
    });
    streamApp = TaskFactoryUtil.createStreamApplication(config);
    assertNull(streamApp);

    config = new MapConfig(new HashMap<>());
    streamApp = TaskFactoryUtil.createStreamApplication(config);
    assertNull(streamApp);
  }

  @Test
  public void testCreateStreamApplicationWithTaskClass() throws Exception {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    StreamApplication streamApp = TaskFactoryUtil.createStreamApplication(config);
    assertNotNull(streamApp);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("should have failed with invalid config");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("should have failed with invalid config");
    } catch (ConfigException ce) {
      // expected
    }


    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "");
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.TestStreamApplication");
      }
    });
    streamApp = TaskFactoryUtil.createStreamApplication(config);
    assertNotNull(streamApp);

  }

  @Test
  public void testStreamTaskClassWithInvalidStreamApplication() throws Exception {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
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
    Object retFactory = TaskFactoryUtil.createTaskFactory(config);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactoryUtil.createTaskFactory(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTaskWithInvalidStreamGraphBuilder() throws Exception {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(ApplicationConfig.APP_CLASS, "org.apache.samza.testUtils.InvalidStreamApplication");
      }
    });
    try {
      TaskFactoryUtil.createStreamApplication(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testFinalizeTaskFactory() throws NoSuchFieldException, IllegalAccessException {
    Object mockFactory = mock(Object.class);
    try {
      TaskFactoryUtil.finalizeTaskFactory(mockFactory, true, null);
      fail("Should have failed with validation");
    } catch (SamzaException se) {
      // expected
    }
    StreamTaskFactory mockStreamFactory = mock(StreamTaskFactory.class);
    Object retFactory = TaskFactoryUtil.finalizeTaskFactory(mockStreamFactory, true, null);
    assertEquals(retFactory, mockStreamFactory);

    ExecutorService mockThreadPool = mock(ExecutorService.class);
    retFactory = TaskFactoryUtil.finalizeTaskFactory(mockStreamFactory, false, mockThreadPool);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof AsyncStreamTaskAdapter);
    AsyncStreamTaskAdapter taskAdapter = (AsyncStreamTaskAdapter) ((AsyncStreamTaskFactory) retFactory).createInstance();
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
