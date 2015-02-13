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

import com.google.common.collect.Maps;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;
import java.util.Map;

public class SamzaQueryPreparingStatement extends Prepare implements RelOptTable.ViewExpander {
  private final RelOptPlanner planner;
  private final RexBuilder rexBuilder;
  protected final CalciteSchema schema;
  protected final RelDataTypeFactory typeFactory;
  private final EnumerableRel.Prefer prefer;
  private final Map<String, Object> internalParameters =
      Maps.newLinkedHashMap();
  private int expansionDepth;
  private SqlValidator sqlValidator;

  public SamzaQueryPreparingStatement(CalcitePrepare.Context context, CatalogReader catalogReader,
                                      RelDataTypeFactory typeFactory,
                                      CalciteSchema schema,
                                      EnumerableRel.Prefer prefer,
                                      RelOptPlanner planner,
                                      Convention resultConvention) {
    super(context, catalogReader, resultConvention);
    this.schema = schema;
    this.prefer = prefer;
    this.planner = planner;
    this.typeFactory = typeFactory;
    this.rexBuilder = new RexBuilder(typeFactory);
  }

  @Override
  protected PreparedResult createPreparedExplanation(RelDataType resultType, RelDataType parameterRowType, RelNode rootRel, boolean explainAsXml, SqlExplainLevel detailLevel) {
    return null;
  }

  @Override
  protected PreparedResult implement(RelDataType rowType, RelNode rootRel, SqlKind sqlKind) {
    return null;
  }

  @Override
  protected SqlToRelConverter getSqlToRelConverter(SqlValidator validator, CatalogReader catalogReader) {
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(
            this, validator, catalogReader, planner, rexBuilder,
            StandardConvertletTable.INSTANCE);
    sqlToRelConverter.setTrimUnusedFields(true);
    return sqlToRelConverter;
  }

  @Override
  public RelNode flattenTypes(RelNode rootRel, boolean restructure) {
    return null;
  }

  @Override
  protected RelNode decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel) {
    return null;
  }

  @Override
  protected void init(Class runtimeContextClass) {}

  @Override
  protected SqlValidator getSqlValidator() {
    return null;
  }

  @Override
  public RelNode expandView(RelDataType rowType, String queryString, List<String> schemaPath) {
    // TODO: Implement custom view expansions
    return super.expandView(rowType, queryString, schemaPath);
  }
}
