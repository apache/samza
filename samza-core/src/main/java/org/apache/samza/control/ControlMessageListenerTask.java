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

import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * The listener interface for the aggregation result of control messages.
 * Any task that handles control messages such as {@link org.apache.samza.message.EndOfStreamMessage}
 * and {@link org.apache.samza.message.WatermarkMessage} needs to implement this interface.
 */
public interface ControlMessageListenerTask {

  /**
   * Returns the topology of the streams. Any control message listener needs to
   * provide this topology so Samza can propagate the control message to downstreams.
   * @return {@link IOGraph} of input to output streams. It
   */
  IOGraph getIOGraph();

  /**
   * Invoked when a Watermark comes.
   * @param watermark contains the watermark timestamp
   * @param systemStream source of stream that emits the watermark
   * @param collector message collector
   * @param coordinator task coordinator
   */
  void onWatermark(Watermark watermark, SystemStream systemStream, MessageCollector collector, TaskCoordinator coordinator);
}
