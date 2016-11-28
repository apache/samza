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
import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;


/**
 * A {@link StreamOperatorTask} is the basic interface to implement for processing {@link MessageStream}s.
 * Implementations can describe the transformation steps for each {@link MessageStream} in the
 * {@link #transform} method using {@link MessageStream} APIs.
 * <p>
 * Implementations may be augmented by implementing {@link org.apache.samza.task.InitableTask},
 * {@link org.apache.samza.task.WindowableTask} and {@link org.apache.samza.task.ClosableTask} interfaces,
 * but should not implement {@link org.apache.samza.task.StreamTask} or {@link org.apache.samza.task.AsyncStreamTask}
 * interfaces.
 */
@InterfaceStability.Unstable
public interface StreamOperatorTask {

  /**
   * Describe the transformation steps for each {@link MessageStream}s for this task using the
   * {@link MessageStream} APIs. Each {@link MessageStream} corresponds to one {@link SystemStreamPartition}
   * in the input system.
   *
   * @param messageStreams the {@link MessageStream}s that receive {@link IncomingSystemMessage}s
   *                       from their corresponding {@link org.apache.samza.system.SystemStreamPartition}
   */
  void transform(Map<SystemStreamPartition, MessageStream<IncomingSystemMessage>> messageStreams);

}
