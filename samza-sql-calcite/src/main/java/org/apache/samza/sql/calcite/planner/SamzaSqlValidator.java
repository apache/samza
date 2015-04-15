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
package org.apache.samza.sql.calcite.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * Defines a Samza specific SQL validator based on Calcite's SQL validator implementation.
 */
public class SamzaSqlValidator extends SqlValidatorImpl{
  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   */
  protected SamzaSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
    /* Note: We may need to define Samza specific SqlConformance instance in future. */
    super(opTab, catalogReader, typeFactory, SqlConformance.DEFAULT);
  }

  @Override
  protected RelDataType getLogicalSourceRowType(
      RelDataType sourceRowType, SqlInsert insert) {
    return ((JavaTypeFactory) typeFactory).toSql(sourceRowType);
  }

  @Override
  protected RelDataType getLogicalTargetRowType(
      RelDataType targetRowType, SqlInsert insert) {
    return ((JavaTypeFactory) typeFactory).toSql(targetRowType);
  }
}
