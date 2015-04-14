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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

/**
 * Streaming query planner implementation based on Calcite.
 */
public class QueryPlanner {
  public static final boolean COMMUTE =
      "true".equals(
          System.getProperties().getProperty("calcite.enable.join.commute"));

  /**
   * Whether to enable the collation trait. Some extra optimizations are
   * possible if enabled, but queries should work either way. At some point
   * this will become a preference, or we will run multiple phases: first
   * disabled, then enabled.
   */
  private static final boolean ENABLE_COLLATION_TRAIT = true;

  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
          COMMUTE
              ? JoinAssociateRule.INSTANCE
              : ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE);

  private static final List<RelOptRule> ENUMERABLE_RULES =
      ImmutableList.of(
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE);

  /**
   * Transform streaming query to a query plan.
   * @param query streaming query in SQL with streaming extensions
   * @param context query prepare context
   * @return query plan
   */
  public RelNode getPlan(String query, CalcitePrepare.Context context) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final CalciteConnectionConfig config = context.config();

    CalciteCatalogReader catalogReader = new CalciteCatalogReader(context.getRootSchema(),
        false,
        context.getDefaultSchemaPath(),
        typeFactory);

    SqlParser sqlParser = SqlParser.create(query,
        SqlParser.configBuilder()
            .setQuotedCasing(config.quotedCasing())
            .setUnquotedCasing(config.unquotedCasing())
            .setQuoting(config.quoting())
            .build());

    SqlNode sqlNode;

    try {
      sqlNode = sqlParser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed: " + e.getMessage(), e);
    }

    final ChainedSqlOperatorTable operatorTable =
        new ChainedSqlOperatorTable(
            ImmutableList.of(SqlStdOperatorTable.instance(), catalogReader));

    final SqlValidator validator =
        new SamzaSqlValidator(operatorTable, catalogReader, typeFactory);
    validator.setIdentifierExpansion(true);

    SqlNode validatedSqlNode = validator.validate(sqlNode);

    final RelOptPlanner planner = createStreamingRelOptPlanner(context, null, null);

    final SamzaQueryPreparingStatement preparingStmt =
        new SamzaQueryPreparingStatement(
            context,
            catalogReader,
            typeFactory,
            context.getRootSchema(),
            EnumerableRel.Prefer.ARRAY,
            planner,
            EnumerableConvention.INSTANCE);

    /* TODO: Add query optimization. */

    return preparingStmt.getSqlToRelConverter(validator, catalogReader).convertQuery(validatedSqlNode, false, true);
  }

  /**
   * Creates a query planner and initializes it with a default set of
   * rules.
   *
   * @param prepareContext  context for preparing a statement
   * @param externalContext external query planning context
   * @param costFactory     cost factory for cost based query planning
   * @return relation query planner instance
   */
  protected RelOptPlanner createStreamingRelOptPlanner(final CalcitePrepare.Context prepareContext,
                                                       org.apache.calcite.plan.Context externalContext,
                                                       RelOptCostFactory costFactory) {
    if (externalContext == null) {
      externalContext = Contexts.withConfig(prepareContext.config());
    }

    final VolcanoPlanner planner =
        new VolcanoPlanner(costFactory, externalContext);

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    if (ENABLE_COLLATION_TRAIT) {
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      planner.registerAbstractRelationalRules();
    }
    RelOptUtil.registerAbstractRels(planner);
    for (RelOptRule rule : DEFAULT_RULES) {
      planner.addRule(rule);
    }

    /* Note: Bindable rules were removed until Calcite switches the convention of the root node to bindable. */

    for (RelOptRule rule : ENUMERABLE_RULES) {
      planner.addRule(rule);
    }

    for (RelOptRule rule : StreamRules.RULES) {
      planner.addRule(rule);
    }

    /* Note: Constant reduction rules were removed because current Calcite implementation doesn't use them. */

    return planner;
  }

}
