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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.util.ReflectionUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.config.JobConfig.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests {@link DefaultTaskExecutorFactory}.
 */
public class TestDefaultTaskExecutorFactory {

  @Test
  public void testGetTaskExecutor() {
    DefaultTaskExecutorFactory factory = new DefaultTaskExecutorFactory();

    Map<String, String> mapConfig = new HashMap<>();
    int poolSize = 12;
    mapConfig.put(JOB_CONTAINER_THREAD_POOL_SIZE, String.valueOf(poolSize));
    Config config = new MapConfig(mapConfig);

    ExecutorService executor = factory.getTaskExecutor(config);

    assertEquals(poolSize, ((ThreadPoolExecutor) executor).getCorePoolSize());
  }

  @Test
  public void testGetTaskExecutorFactory() {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put(JOB_CONTAINER_TASK_EXECUTOR_FACTORY, MockTaskExecutorFactory.class.getName());
    JobConfig config = new JobConfig(new MapConfig(mapConfig));

    String taskExecutorFactoryClassName = config.getTaskExecutorFactory();
    TaskExecutorFactory taskExecutorFactory = ReflectionUtil.getObj(taskExecutorFactoryClassName, TaskExecutorFactory.class);

    Assert.assertTrue(taskExecutorFactory instanceof MockTaskExecutorFactory);
  }

  @Test
  public void getOperatorTaskFactoryWithContainerThreadPoolGreaterThanOne() {
    DefaultTaskExecutorFactory factory = new DefaultTaskExecutorFactory();
    int poolSize = 5;
    Map<String, String> config = ImmutableMap.of(JOB_CONTAINER_THREAD_POOL_SIZE, Integer.toString(poolSize));

    ExecutorService operatorExecutor = factory.getOperatorExecutor(mock(TaskName.class), new MapConfig(config));
    assertEquals(poolSize, ((ThreadPoolExecutor) operatorExecutor).getCorePoolSize());
  }

  @Test
  public void getOperatorTaskFactoryWithThreadContainerPoolLessThanOne() {
    DefaultTaskExecutorFactory factory = new DefaultTaskExecutorFactory();
    Map<String, String> config = ImmutableMap.of(JOB_CONTAINER_THREAD_POOL_SIZE, "-1");

    ExecutorService operatorExecutor = factory.getOperatorExecutor(mock(TaskName.class), new MapConfig(config));
    assertFalse(operatorExecutor instanceof ThreadPoolExecutor);
  }

  @Test
  public void getOperatorTaskFactoryForSameTask() {
    DefaultTaskExecutorFactory factory = new DefaultTaskExecutorFactory();
    TaskName taskName = mock(TaskName.class);
    Map<String, String> config = ImmutableMap.of(JOB_CONTAINER_THREAD_POOL_SIZE, "0");

    ExecutorService taskExecutor = factory.getOperatorExecutor(taskName, new MapConfig(config));
    assertEquals("Expected same executor instance to be returned for the same task within a JVM instance", taskExecutor,
        factory.getOperatorExecutor(taskName, new MapConfig(config)));
  }

  public static class MockTaskExecutorFactory implements TaskExecutorFactory {

    @Override
    public ExecutorService getTaskExecutor(Config config) {
      return null;
    }
  }
}
