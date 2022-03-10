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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.ReflectionUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.config.JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE;
import static org.apache.samza.config.JobConfig.JOB_CONTAINER_TASK_EXECUTOR_FACTORY;


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

    Assert.assertEquals(poolSize, ((ThreadPoolExecutor) executor).getCorePoolSize());
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

  public static class MockTaskExecutorFactory implements TaskExecutorFactory {

    @Override
    public ExecutorService getTaskExecutor(Config config) {
      return null;
    }
  }
}
