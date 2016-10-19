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

package org.apache.samza.container;

import java.util.concurrent.ExecutorService;
import org.apache.samza.SamzaException;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.task.AsyncRunLoop;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import static org.apache.samza.util.Utils.defaultClock;
import static org.apache.samza.util.Utils.defaultValue;

/**
 * Factory class to create runloop for a Samza task, based on the type
 * of the task
 */
public class RunLoopFactory {
  private static final Logger log = LoggerFactory.getLogger(RunLoopFactory.class);

  private static final long DEFAULT_WINDOW_MS = -1L;
  private static final long DEFAULT_COMMIT_MS = 60000L;
  private static final long DEFAULT_CALLBACK_TIMEOUT_MS = -1L;

  public static Runnable createRunLoop(scala.collection.immutable.Map<TaskName, TaskInstance<?>> taskInstances,
      SystemConsumers consumerMultiplexer,
      ExecutorService threadPool,
      long maxThrottlingDelayMs,
      SamzaContainerMetrics containerMetrics,
      TaskConfig config) {

    long taskWindowMs = config.getWindowMs().getOrElse(defaultValue(DEFAULT_WINDOW_MS));

    log.info("Got window milliseconds: " + taskWindowMs);

    long taskCommitMs = config.getCommitMs().getOrElse(defaultValue(DEFAULT_COMMIT_MS));

    log.info("Got commit milliseconds: " + taskCommitMs);

    int asyncTaskCount = taskInstances.values().count(new AbstractFunction1<TaskInstance<?>, Object>() {
      @Override
      public Boolean apply(TaskInstance<?> t) {
        return t.isAsyncTask();
      }
    });

    // asyncTaskCount should be either 0 or the number of all taskInstances
    if (asyncTaskCount > 0 && asyncTaskCount < taskInstances.size()) {
      throw new SamzaException("Mixing StreamTask and AsyncStreamTask is not supported");
    }

    if (asyncTaskCount == 0) {
      log.info("Run loop in single thread mode.");

      scala.collection.immutable.Map<TaskName, TaskInstance<StreamTask>> streamTaskInstances = (scala.collection.immutable.Map) taskInstances;
      return new RunLoop(
        streamTaskInstances,
        consumerMultiplexer,
        containerMetrics,
        maxThrottlingDelayMs,
        taskWindowMs,
        taskCommitMs,
        defaultClock());
    } else {
      Integer taskMaxConcurrency = config.getMaxConcurrency().getOrElse(defaultValue(1));

      log.info("Got max messages in flight: " + taskMaxConcurrency);

      Long callbackTimeout = config.getCallbackTimeoutMs().getOrElse(defaultValue(DEFAULT_CALLBACK_TIMEOUT_MS));

      log.info("Got callback timeout: " + callbackTimeout);

      scala.collection.immutable.Map<TaskName, TaskInstance<AsyncStreamTask>> asyncStreamTaskInstances = (scala.collection.immutable.Map) taskInstances;

      log.info("Run loop in asynchronous mode.");

      return new AsyncRunLoop(
        JavaConversions.asJavaMap(asyncStreamTaskInstances),
        threadPool,
        consumerMultiplexer,
        taskMaxConcurrency,
        taskWindowMs,
        taskCommitMs,
        callbackTimeout,
        maxThrottlingDelayMs,
        containerMetrics);
    }
  }
}
