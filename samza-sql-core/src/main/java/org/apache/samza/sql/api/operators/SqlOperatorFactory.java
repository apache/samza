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

import org.apache.samza.sql.api.operators.spec.OperatorSpec;


/**
 * This class defines the interface of SQL operator factory, which creates the following operators:
 * <ul>
 * <li><code>RelationOperator</code> that takes <code>Relation</code> as input variables
 * <li><code>TupleOperator</code> that takes <code>Tuple</code> as input variables
 * </ul>
 *
 */
public interface SqlOperatorFactory {

  /**
   * Interface method to create/get the <code>RelationOperator</code> object
   *
   * @param spec The specification of the <code>RelationOperator</code> object
   * @return The relation operator object
   */
  RelationOperator getRelationOperator(OperatorSpec spec);

  /**
   * Interface method to create/get the <code>TupleOperator</code> object
   *
   * @param spec The specification of the <code>TupleOperator</code> object
   * @return The tuple operator object
   */
  TupleOperator getTupleOperator(OperatorSpec spec);

}
