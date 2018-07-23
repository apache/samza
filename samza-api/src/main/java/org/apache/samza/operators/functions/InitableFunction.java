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

package org.apache.samza.operators.functions;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.StreamApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;

/**
 * A function that can be initialized before execution.
 *
 * <p> Order of initialization: {@link InitableFunction}s are invoked in the topological order of operators in the
 * {@link StreamApplicationSpec}. For any two operators A and B in the graph, if operator B consumes results
 * from operator A, then operator A is guaranteed to be initialized before operator B.
 *
 */
@InterfaceStability.Unstable
public interface InitableFunction {

  /**
   * Initializes the function before any messages are processed.
   *
   * @param config the {@link Config} for the application
   * @param context the {@link TaskContext} for this task
   */
  default void init(Config config, TaskContext context) { }
}
