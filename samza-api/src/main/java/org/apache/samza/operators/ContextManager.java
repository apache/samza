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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Interface class defining methods to initialize and finalize the context used by the transformation functions.
 */
@InterfaceStability.Unstable
public interface ContextManager {
  /**
   * The initialization method to create shared context for the whole task in Samza. Default to NO-OP
   *
   * @param config  the configuration object for the task
   * @param context  the {@link TaskContext} object
   * @return  User-defined task-wide context object
   */
  default TaskContext initTaskContext(Config config, TaskContext context) {
    return context;
  }

  /**
   * The finalize method to allow users to close resource initialized in {@link #initTaskContext} method. Default to NO-OP.
   *
   */
  default void finalizeTaskContext() { }
}
