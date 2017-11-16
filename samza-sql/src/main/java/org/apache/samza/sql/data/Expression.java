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

package org.apache.samza.sql.data;

import org.apache.calcite.DataContext;


/**
 * {@link RexToJavaCompiler} creates the Java code for each relational expression at runtime.
 * This is the interface which the runtime generated java code for each Relational expression should implement.
 */
public interface Expression {
  /**
   * This method is used to implement the expressions that takes in columns as input and returns multiple values.
   * @param context the context
   * @param root the root
   * @param inputValues All the relational columns for the particular row
   * @param results the results Result values after executing the java code corresponding to the relational expression.
   */
  void execute(SamzaSqlExecutionContext context, DataContext root, Object[] inputValues, Object[] results);
}
