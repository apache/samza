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

import java.io.Serializable;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * Allows sending a message to an output system.
 *
 * @param <M>  type of the input message
 */
@InterfaceStability.Unstable
@FunctionalInterface
public interface SinkFunction<M>  extends InitableFunction, ClosableFunction, Serializable {

  /**
   * Allows sending the provided message to an output {@link org.apache.samza.system.SystemStream} using
   * the provided {@link MessageCollector}. Also provides access to the {@link TaskCoordinator} to request commits
   * or shut the container down.
   *
   * @param message  the input message to be sent to an output {@link org.apache.samza.system.SystemStream}
   * @param messageCollector  the {@link MessageCollector} to send the message
   * @param taskCoordinator  the {@link TaskCoordinator} to request commits or shutdown
   */
  void apply(M message, MessageCollector messageCollector, TaskCoordinator taskCoordinator);

}
