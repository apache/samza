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
 * Joins incoming messages with records from a table by the join key.
 *
 * @param <K> the type of join key
 * @param <M>  type of input message
 * @param <R>  type of the table record
 * @param <JM> type of join results
 */
@InterfaceStability.Unstable
public interface StreamTableJoinFunction<K, M, R, JM> extends InitableFunction, ClosableFunction {

  /**
   * Joins the provided messages and table record, returns the joined message.
   *
   * @param message  the input message
   * @param record  the table record value
   * @return  the join result
   */
  JM apply(M message, R record);

  /**
   * Retrieve the join key from incoming messages
   *
   * @param message incoming message
   * @return the join key
   */
  K getMessageKey(M message);

  /**
   * Retrieve the join key from table record
   *
   * @param record table record
   * @return the join key
   */
  K getRecordKey(R record);
}
