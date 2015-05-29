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

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

/**
 * Defines the callback functions to allow customized functions to be invoked before process and before sending the result
 */
public interface OperatorCallback {
  /**
   * Method to be invoked before the operator actually process the input tuple
   *
   * @param tuple The incoming tuple
   * @param collector The {@link org.apache.samza.task.MessageCollector} in context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in context
   * @return The tuple to be processed; return {@code null} if there is nothing to be processed
   */
  Tuple beforeProcess(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Method to be invoked before the operator actually process the input relation
   *
   * @param rel The input relation
   * @param collector The {@link org.apache.samza.task.MessageCollector} in context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in context
   * @return The relation to be processed; return {@code null} if there is nothing to be processed
   */
  Relation beforeProcess(Relation rel, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Method to be invoked before the operator's output tuple is sent
   *
   * @param tuple The output tuple
   * @param collector The {@link org.apache.samza.task.MessageCollector} in context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in context
   * @return The tuple to be sent; return {@code null} if there is nothing to be sent
   */
  Tuple afterProcess(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Method to be invoked before the operator's output relation is sent
   *
   * @param rel The output relation
   * @param collector The {@link org.apache.samza.task.MessageCollector} in context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in context
   * @return The relation to be sent; return {@code null} if there is nothing to be sent
   */
  Relation afterProcess(Relation rel, MessageCollector collector, TaskCoordinator coordinator);
}
