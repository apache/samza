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

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;


/**
 * This interface class defines interface methods to connect {@link org.apache.samza.sql.api.operators.SimpleOperator}s together into a composite operator.
 *
 * <p>The {@code OperatorRouter} allows the user to attach operators to a {@link org.apache.samza.sql.api.data.Table} or
 * a {@link org.apache.samza.sql.api.data.Stream} entity, if the corresponding table/stream is included as inputs to the operator.
 * Each operator then executes its own logic and determines which table/stream to emit the output to. Through the {@code OperatorRouter},
 * the next operators attached to the corresponding output entities (i.e. table/streams) can then be invoked to continue the
 * stream process task.
 */
public interface OperatorRouter extends Operator {

  /**
   * This method adds a {@link org.apache.samza.sql.api.operators.SimpleOperator} to the {@code OperatorRouter}.
   *
   * @param nextOp The {@link org.apache.samza.sql.api.operators.SimpleOperator} to be added
   * @throws Exception Throws exception if failed
   */
  void addOperator(SimpleOperator nextOp) throws Exception;

  /**
   * This method gets the list of {@link org.apache.samza.sql.api.operators.SimpleOperator}s attached to an output entity (of any type)
   *
   * @param output The identifier of the output entity
   * @return The list of {@link org.apache.samza.sql.api.operators.SimpleOperator} taking {@code output} as input table/stream
   */
  List<SimpleOperator> getNextOperators(EntityName output);

}
