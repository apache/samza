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

package org.apache.samza.sql.api.operators.spec;

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;


/**
 * This class defines a generic specification interface class for all operators.
 *
 * <p>The purpose of this class is to encapsulate all the details of configuration/parameters of a specific implementation of an operator.
 *
 * <p>The generic methods for an operator specification is to provide methods to get the unique ID, the list of entity names (i.e. stream name
 * in <code>Tuple</code> or <code>Relation</code> name) of input variables , and the list of entity names of the output variables.
 *
 */
public interface OperatorSpec {
  /**
   * Interface method that returns the unique ID of the operator in a task
   *
   * @return The unique ID of the <code>Operator</code> object
   */
  String getId();

  /**
   * Access method to the list of entity names of input variables.
   *
   * <p>The input entity names are either stream names if the operator is a <code>TupleOperator</code>;
   * or <code>Relation</code> names if the operator is a <code>RelationOperator</code>
   *
   * @return A list of entity names of the inputs
   */
  List<EntityName> getInputNames();

  /**
   * Access method to the list of entity name of the output variable
   *
   * <p>The output entity name is either a stream name if the operator generates tuples as an output stream;
   * or <code>Relation</code> names if the operator generates a <code>Relation</code> as output.
   *
   * @return The entity name of the output
   *
   */
  List<EntityName> getOutputNames();
}
