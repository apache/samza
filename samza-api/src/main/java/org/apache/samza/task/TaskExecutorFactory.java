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

import java.util.concurrent.ExecutorService;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;


/**
 * Factory for creating the executor used when running tasks in multi-thread mode.
 */
@InterfaceStability.Unstable
public interface TaskExecutorFactory {

  /**
   * @param config contains configs for the executor
   * @return task executor
   */
  ExecutorService getTaskExecutor(Config config);

  /**
   * Operator thread pool is asynchronous execution facility used to execute hand-off between operators and sub-DAG in case
   * of synchronous operators in the application DAG. In case of asynchronous operators, typically the operator invocation
   * happens on one thread while completion of the callback happens on another thread. When the CompletionStage completes normally,
   * the subsequent DAG or hand-off code is executed on the operator thread pool.
   * <b>Note:</b>It is up to the implementors of the factory to share the executor across tasks vs provide isolated executors
   * per task. While the above determines fairness and contention between tasks, within a task
   * there are no fairness guarantees on how the chained futures of sub-DAG stages are executed nor any guarantees on
   * the order of tasks executed. The behavior is controlled by Java's implementation of CompletionStage and
   * CompletableFuture which determines what thread and how tasks post completion of futures execute.
   *
   * @param taskName name of the task
   * @param config application configuration
   * @return an {@link ExecutorService} to run samza operator related tasks
   */
  default ExecutorService getOperatorExecutor(TaskName taskName, Config config) {
    return getTaskExecutor(config);
  }
}
