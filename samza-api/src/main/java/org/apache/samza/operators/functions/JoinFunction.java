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
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * A function that joins messages from two {@link org.apache.samza.operators.MessageStream}s and produces
 * a joined message.
 * @param <K>  type of the join key
 * @param <M>  type of the input message
 * @param <JM>  type of the message to join with
 * @param <RM>  type of the joined message
 */
@InterfaceStability.Unstable
public interface JoinFunction<K, M, JM, RM>  extends InitFunction {

  /**
   * Join the provided input messages and produces the joined messages.
   * @param message  the input message
   * @param otherMessage  the message to join with
   * @return  the joined message
   */
  RM apply(M message, JM otherMessage);

  /**
   * Method to get the join key in the messages from the first input stream
   *
   * @param message  the input message from the first input stream
   * @return  the join key
   */
  K getFirstKey(M message);

  /**
   * Method to get the join key in the messages from the second input stream
   *
   * @param message  the input message from the second input stream
   * @return  the join key
   */
  K getSecondKey(JM message);

  /**
   * Init method to initialize the context for this {@link JoinFunction}. The default implementation is NO-OP.
   *
   * @param config  the {@link Config} object for this task
   * @param context  the {@link TaskContext} object for this task
   */
  default void init(Config config, TaskContext context) { }
}
