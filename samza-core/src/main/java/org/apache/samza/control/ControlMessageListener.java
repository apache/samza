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

package org.apache.samza.control;

import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * This interface provides the callback listener for the aggregation result of control messages
 */
public interface ControlMessageListener {

  /**
   * Invoked when an EndOfStream comes
   * @param endOfStream contains the stream that reaches to the end.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  void onEndOfStream(EndOfStream endOfStream, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Invoked when a Watermark comes
   * @param watermark contains the watermark timestamp
   * @param collector message collector
   * @param coordinator task coordinator
   */
  void onWatermark(Watermark watermark, MessageCollector collector, TaskCoordinator coordinator);
}
