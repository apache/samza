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

package org.apache.samza.sql.api.operators;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


public interface Operator {
  /**
   * Method to initialize the operator
   *
   * @param config The configuration object
   * @param context The task context
   * @throws Exception Throws Exception if failed to initialize the operator
   */
  void init(Config config, TaskContext context) throws Exception;

  /**
   * Method to perform a relational logic on the input relation
   *
   * <p> The actual implementation of relational logic is performed by the implementation of this method.
   *
   * @param deltaRelation The changed rows in the input relation, including the inserts/deletes/updates
   * @param collector The {@link org.apache.samza.task.MessageCollector} that accepts outputs from the operator
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @throws Exception Throws exception if failed
   */
  void process(Relation deltaRelation, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception;

  /**
   * Method to process on an input tuple.
   *
   * @param tuple The input tuple, which has the incoming message from a stream
   * @param collector The {@link org.apache.samza.task.MessageCollector} that accepts outputs from the operator
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @throws Exception Throws exception if failed
   */
  void process(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) throws Exception;

  /**
   * Method to refresh the result when a timer expires
   *
   * @param timeNano The current system time in nano second
   * @param collector The {@link org.apache.samza.task.MessageCollector} that accepts outputs from the operator
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @throws Exception Throws exception if failed
   */
  void refresh(long timeNano, MessageCollector collector, TaskCoordinator coordinator) throws Exception;

}
