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

/**
 * Used as an interface for user processing StreamTasks that need to have specific functionality performed as their StreamTasks
 * are instantiated by TaskRunner.
 */
public interface InitableTask {
  /**
   * Called by TaskRunner each time an implementing task is created.
   * @param config Allows accessing of fields in the configuration files that this StreamTask is specified in.
   * @param context Allows accessing of contextual data of this StreamTask.
   * @throws Exception Any exception types encountered during the execution of the processing task.
   */
  void init(Config config, TaskContext context) throws Exception;
}
