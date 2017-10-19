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
package org.apache.samza.operators;

import java.io.Serializable;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Manages custom context that is shared across multiple operator functions in a task.
 */
@InterfaceStability.Unstable
public interface ContextManager extends Serializable {

  /**
   * Allows initializing and setting a custom context that is shared across multiple operator functions in a task.
   * <p>
   * This method is invoked before any {@link org.apache.samza.operators.functions.InitableFunction}s are initialized.
   * Use {@link TaskContext#setUserContext(Object)} to set the context here and {@link TaskContext#getUserContext()} to
   * get it in InitableFunctions.
   *
   * @param config the {@link Config} for the application
   * @param context the {@link TaskContext} for this task
   */
  void init(Config config, TaskContext context);

  /**
   * Allows closing the custom context that is shared across multiple operator functions in a task.
   */
  void close();

  /**
   * Placeholder, user-inject context could be implemented in three levels:
   * <ul>
   *   <li>in a single operator instance</li>
   *   <li>in a single task instance, among different operators</li>
   *   <li>in a single physical process, among different task instances</li>
   * </ul>
   *
   * Case 1 in the above list is handled by {@link org.apache.samza.operators.functions.InitableFunction} interface, while
   * case 2 and 3 needs to be handled at task/process level here.
   *
   * @return an implementation of {@link ContextManager} interface that deals w/ per task context manager
   */
  ContextManager getContextManagerPerTask();

  /**
   * Placeholder, method to get the {@link ContextManager} that deals w/ per processor context manager
   *
   * @return an implementation of {@link ContextManager} interface that deals w/ per processor context manager
   */
  ContextManager getContextManagerPerProcessor();
}
