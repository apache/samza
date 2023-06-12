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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default factory for creating the executor used when running tasks in multi-thread mode.
 */
public class DefaultTaskExecutorFactory implements TaskExecutorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskExecutorFactory.class);

  private static final Map<TaskName, ExecutorService> TASK_EXECUTORS = new ConcurrentHashMap<>();

  @Override
  public ExecutorService getTaskExecutor(Config config) {
    int threadPoolSize = new JobConfig(config).getThreadPoolSize();

    return Executors.newFixedThreadPool(threadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("Samza Container Thread-%d").build());
  }

  /**
   * {@inheritDoc}
   *
   * The choice of thread pool is determined based on the following logic
   *    If job.operator.thread.pool.enabled,
   *     a. Use {@link #getTaskExecutor(Config)} if job.container.thread.pool.size &gt; 1
   *     b. Use default single threaded pool otherwise
   * <b>Note:</b> The default single threaded pool used is a substitute for the scenario where container thread pool is null and
   * the messages are dispatched on runloop thread. We can't have the stages schedule on the run loop thread and hence
   * the fallback to use a single threaded executor across all tasks.
   */
  @Override
  public ExecutorService getOperatorExecutor(TaskName taskName, Config config) {
    ExecutorService taskExecutor = TASK_EXECUTORS.computeIfAbsent(taskName, key -> {
      final int threadPoolSize = new JobConfig(config).getThreadPoolSize();
      ExecutorService operatorExecutor;

      if (threadPoolSize > 1) {
        LOG.info("Using container thread pool as operator thread pool for task {}", key.getTaskName());
        operatorExecutor = getTaskExecutor(config);
      } else {
        LOG.info("Using single threaded thread pool as operator thread pool for task {}", key.getTaskName());
        operatorExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("Samza " + key.getTaskName() + " Thread-%d").build());
      }

      return operatorExecutor;
    });

    return taskExecutor;
  }
}
