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


/**
 * Joins incoming messages in two streams by key.
 *
 * @param <K>  type of the join key
 * @param <M>  type of the input message
 * @param <JM>  type of the message to join with
 * @param <RM>  type of the joined message
 */
@InterfaceStability.Unstable
public interface JoinFunction<K, M, JM, RM>  extends InitableFunction {

  /**
   * Joins the provided messages and returns the joined message.
   *
   * @param message  the input message
   * @param otherMessage  the message to join with
   * @return  the joined message
   */
  RM apply(M message, JM otherMessage);

  /**
   * Get the join key for messages in the first input stream.
   *
   * @param message  the message in the first input stream
   * @return  the join key
   */
  K getFirstKey(M message);

  /**
   * Get the join key for messages in the second input stream.
   *
   * @param message  the message in the second input stream
   * @return  the join key
   */
  K getSecondKey(JM message);

}
