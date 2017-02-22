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

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.testUtils.TestAsyncStreamTask;
import org.apache.samza.testUtils.TestStreamTask;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test methods to create {@link StreamTaskFactory} or {@link AsyncStreamTaskFactory} based on task class configuration
 */
public class TestTaskFactories {

  @Test
  public void testStreamTaskClass() throws ClassNotFoundException {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ no.such.class");
    } catch (ClassNotFoundException cfe) {
      // expected
    }
  }

  @Test
  public void testStreamOperatorTaskClass() throws ClassNotFoundException {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamGraphBuilder");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamGraphBuilder");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "no.such.class");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ empty class name for StreamGraphBuilder");
    } catch (ConfigException ce) {
      // expected
    }

    config = new MapConfig(new HashMap<String, String>());
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail(String.format("Should have failed w/ non-existing entry for %s", StreamGraphBuilder.BUILDER_CLASS_CONFIG));
    } catch (ConfigException ce) {
      // expected
    }
  }

  @Test
  public void testStreamOperatorTaskClassWithTaskClass() throws ClassNotFoundException {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamGraphBuilder");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamGraphBuilder");
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.TestStreamGraphBuilder");
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof StreamOperatorTask);
  }

  @Test
  public void testStreamTaskClassWithInvalidStreamGraphBuilder() throws ClassNotFoundException {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamGraphBuilder");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "");
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, null);
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof TestStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamGraphBuilder");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ no class not found");
    } catch (ClassNotFoundException cne) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTask() throws ClassNotFoundException {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "no.such.class");
      }
    });
    try {
      TaskFactories.fromTaskClassConfig(config);
      fail("Should have failed w/ no.such.class");
    } catch (ClassNotFoundException cfe) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTaskWithInvalidStreamGraphBuilder() throws ClassNotFoundException {

    Config config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "org.apache.samza.testUtils.InvalidStreamGraphBuilder");
      }
    });
    Object retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, "");
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);

    config = new MapConfig(new HashMap<String, String>() {
      {
        this.put("task.class", "org.apache.samza.testUtils.TestAsyncStreamTask");
        this.put(StreamGraphBuilder.BUILDER_CLASS_CONFIG, null);
      }
    });
    retFactory = TaskFactories.fromTaskClassConfig(config);
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof TestAsyncStreamTask);
  }
}
