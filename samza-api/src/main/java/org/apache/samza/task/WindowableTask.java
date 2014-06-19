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
 * Add-on interface to {@link org.apache.samza.task.StreamTask} implementations to add code which will be run on
 * a specified time interval (via configuration).  This can be used to implement direct time-based windowing or,
 * with a frequent window interval, windowing based on some other condition which is checked during the call to
 * window.  The window method will be called even if no messages are received for a particular StreamTask.
 */
public interface WindowableTask {
  /**
   * Called by TaskRunner for each implementing task at the end of every specified window.
   * @param collector Contains the means of sending message envelopes to the output stream. The collector must only
   * be used during the current call to the window method; you should not reuse the collector between invocations
   * of this method.
   *
   * @param coordinator Manages execution of tasks.
   * @throws Exception Any exception types encountered during the execution of the processing task.
   */
  void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
}
