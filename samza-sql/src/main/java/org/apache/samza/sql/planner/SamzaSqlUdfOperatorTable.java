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

package org.apache.samza.sql.planner;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;


public class SamzaSqlUdfOperatorTable implements SqlOperatorTable {

  private final ListSqlOperatorTable operatorTable;

  public SamzaSqlUdfOperatorTable(List<SamzaSqlScalarFunctionImpl> scalarFunctions) {
    operatorTable = new ListSqlOperatorTable(getSqlOperators(scalarFunctions));
  }

  private List<SqlOperator> getSqlOperators(List<SamzaSqlScalarFunctionImpl> scalarFunctions) {
    return scalarFunctions.stream().map(this::getSqlOperator).collect(Collectors.toList());
  }

  private SqlOperator getSqlOperator(SamzaSqlScalarFunctionImpl scalarFunction) {
    return new SqlUserDefinedFunction(new SqlIdentifier(scalarFunction.getUdfName(), SqlParserPos.ZERO),
        o -> scalarFunction.getReturnType(o.getTypeFactory()), null, Checker.ANY_CHECKER, null, scalarFunction);
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList) {
    operatorTable.lookupOperatorOverloads(opName, category, syntax, operatorList);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return operatorTable.getOperatorList();
  }
}
