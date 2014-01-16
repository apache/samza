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

/**
 * Used as a standard interface to allow user processing tasks to operate on specified time intervals, or "windows".
 */
public interface WindowableTask {
  /**
   * Called by TaskRunner for each implementing task at the end of every specified window.
   * @param collector Contains the means of sending message envelopes to the output stream.
   * @param coordinator Manages execution of tasks.
   * @throws Exception Any exception types encountered during the execution of the processing task.
   */
  void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
}
