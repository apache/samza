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
 * Joins incoming messages with records from a table by key.
 *
 * @param <K>  type of the join key
 * @param <M>  type of the input message
 * @param <R>  type of records in the table
 * @param <OM> type of join results
 */
@InterfaceStability.Unstable
public interface StreamTableJoinFunction<K, M, R, OM> extends InitableFunction, ClosableFunction {

  /**
   * Joins the provided messages and table record, returns the joined message.
   *
   * @param message  the input message
   * @param record  the table record
   * @return  the join result
   */
  OM apply(M message, R record);

  /**
   * Get the join key from messages in the input stream.
   *
   * @param message  the message in the input stream
   * @return  the join key
   */
  K getFirstKey(M message);

}
