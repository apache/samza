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

import org.apache.samza.SamzaException;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.task.AsyncRunLoop;
import org.apache.samza.util.HighResolutionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import java.util.concurrent.ExecutorService;

import static org.apache.samza.util.Util.asScalaClock;


/**
 * Factory class to create runloop for a Samza task, based on the type
 * of the task
 */
public class RunLoopFactory {
  private static final Logger log = LoggerFactory.getLogger(RunLoopFactory.class);

  public static Runnable createRunLoop(scala.collection.immutable.Map<TaskName, TaskInstance> taskInstances,
      SystemConsumers consumerMultiplexer,
      ExecutorService threadPool,
      long maxThrottlingDelayMs,
      SamzaContainerMetrics containerMetrics,
      TaskConfig config,
      HighResolutionClock clock) {

    long taskWindowMs = config.getWindowMs();

    log.info("Got window milliseconds: {}.", taskWindowMs);

    long taskCommitMs = config.getCommitMs();

    log.info("Got commit milliseconds: {}.", taskCommitMs);

    int asyncTaskCount = taskInstances.values().count(new AbstractFunction1<TaskInstance, Object>() {
      @Override
      public Boolean apply(TaskInstance t) {
        return t.isAsyncTask();
      }
    });

    // asyncTaskCount should be either 0 or the number of all taskInstances
    if (asyncTaskCount > 0 && asyncTaskCount < taskInstances.size()) {
      throw new SamzaException("Mixing StreamTask and AsyncStreamTask is not supported");
    }

    if (asyncTaskCount == 0) {
      log.info("Run loop in single thread mode.");

      return new RunLoop(
        taskInstances,
        consumerMultiplexer,
        containerMetrics,
        maxThrottlingDelayMs,
        taskWindowMs,
        taskCommitMs,
        asScalaClock(() -> System.nanoTime()));
    } else {
      Integer taskMaxConcurrency = config.getMaxConcurrency();

      log.info("Got taskMaxConcurrency: {}.", taskMaxConcurrency);

      boolean isAsyncCommitEnabled = config.getAsyncCommit();

      log.info("Got asyncCommitEnabled: {}.", isAsyncCommitEnabled);

      Long callbackTimeout = config.getCallbackTimeoutMs();

      log.info("Got callbackTimeout: {}.", callbackTimeout);

      log.info("Run loop in asynchronous mode.");

      return new AsyncRunLoop(
        JavaConverters.mapAsJavaMapConverter(taskInstances).asJava(),
        threadPool,
        consumerMultiplexer,
        taskMaxConcurrency,
        taskWindowMs,
        taskCommitMs,
        callbackTimeout,
        maxThrottlingDelayMs,
        containerMetrics,
        clock,
        isAsyncCommitEnabled);
    }
  }

}
