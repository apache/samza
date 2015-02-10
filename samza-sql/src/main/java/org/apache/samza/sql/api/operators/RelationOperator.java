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
import org.apache.samza.task.sql.SqlMessageCollector;


/**
 * This class defines the interface <code>RelationOperator</code>.
 *
 * <p>All operators implementing <code>RelationOperator</code> will take a <code>Relation</code> object as input.
 * The SQL operators that need to implement this interface include:
 * <ul>
 * <li>All relation algebra operators, such as: join, select, where, group-by, having, limit, order-by, etc.
 * <li>All relation-to-stream operators, which converts a relation to a stream
 * </ul>
 *
 */
public interface RelationOperator extends Operator {

  /**
   * Method to perform a relational algebra on a set of relations, or a relation-to-stream function
   *
   * <p> The actual implementation of relational logic is performed by the implementation of this method.
   * The <code>collector</code> object is used by the operator to send their output to
   *
   * @param deltaRelation The changed rows in the input relation, including the inserts/deletes/updates
   * @param collector The <code>SqlMessageCollector</code> object that accepts outputs from the operator
   * @throws Exception Throws exception if failed
   */
  void process(Relation deltaRelation, SqlMessageCollector collector) throws Exception;

}
