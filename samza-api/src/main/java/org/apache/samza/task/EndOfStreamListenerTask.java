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
 *
 * The EndOfStreamListenerTask augments {@link StreamTask} allowing the method implementor to specify code to be
 * executed when the 'end-of-stream' is reached for all input SSPs.
 *
 * While some streaming sources are infinite (like kafka), some others like HDFS, File based sources are bounded. For instance,
 * file based sources have the notion of EOF to indicate that there is no more data.
 *
 */
public interface EndOfStreamListenerTask {

  /**
   * Guaranteed to be invoked when all SSPs processed by this task have reached their end-of-stream. Users can choose
   * to invoke commit on the {@link TaskCoordinator} to commit changes.
   *
   * @param collector Contains the means of sending message envelopes to an output stream.*
   * @param coordinator Manages execution of tasks.
   *
   * @throws Exception Any exception types encountered during the execution of the processing task.
   */
  void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
}
