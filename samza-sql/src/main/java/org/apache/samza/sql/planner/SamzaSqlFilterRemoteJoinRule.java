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

import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.translator.JoinInputNode;
import org.apache.samza.sql.translator.JoinInputNode.InputType;

/**
 * Planner rule for remote table joins that pushes filters above and
 * within a join node into its children nodes.
 * This class is customized form of Calcite's {@link org.apache.calcite.rel.rules.FilterJoinRule} for
 * remote table joins.
 */
public abstract class SamzaSqlFilterRemoteJoinRule extends RelOptRule {
  /** Whether to try to strengthen join-type. */
  private final boolean smart;

  Map<String, SqlIOConfig> systemStreamConfigBySource;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterJoinRule with an explicit root operand and
   * factories.
   */
  protected SamzaSqlFilterRemoteJoinRule(RelOptRuleOperand operand, String id,
      boolean smart, RelBuilderFactory relBuilderFactory, Map<String, SqlIOConfig> systemStreamConfigBySource) {
    super(operand, relBuilderFactory, "SamzaSqlFilterRemoteJoinRule:" + id);
    this.smart = smart;
    this.systemStreamConfigBySource = systemStreamConfigBySource;
  }

  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, Filter filter,
      Join join) {
    final List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());

    boolean donotOptimizeLeft = false;
    boolean donotOptimizeRight = false;

    JoinInputNode.InputType inputTypeOnLeft =
        JoinInputNode.getInputType(join.getLeft(), systemStreamConfigBySource);
    JoinInputNode.InputType inputTypeOnRight =
        JoinInputNode.getInputType(join.getRight(), systemStreamConfigBySource);

    // Disable this optimnization for queries using local table.
    if (inputTypeOnLeft == InputType.LOCAL_TABLE || inputTypeOnRight == InputType.LOCAL_TABLE) {
      donotOptimizeLeft = true;
      donotOptimizeRight = true;
    }

    // There is nothing to optimize on the remote table side as the lookup needs to happen first before filtering.
    if (inputTypeOnLeft == InputType.REMOTE_TABLE) {
      donotOptimizeLeft = true;
    }
    if (inputTypeOnRight == InputType.REMOTE_TABLE) {
      donotOptimizeRight = true;
    }

    // If there is only the joinRel,
    // make sure it does not match a cartesian product joinRel
    // (with "true" condition), otherwise this rule will be applied
    // again on the new cartesian product joinRel.
    if (filter == null && joinFilters.isEmpty()) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? RelOptUtil.conjunctions(filter.getCondition())
            : new ArrayList<>();
    final com.google.common.collect.ImmutableList<RexNode> origAboveFilters =
        com.google.common.collect.ImmutableList.copyOf(aboveFilters);

    // Simplify Outer Joins
    JoinRelType joinType = join.getJoinType();
    if (smart
        && !origAboveFilters.isEmpty()
        && join.getJoinType() != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
    }

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    // We do not push into join condition as we do not benefit much. There is also correctness issue
    // with remote table as we will not have values for the remote table before the join/lookup.
    boolean filterPushed = false;
    if (RelOptUtil.classifyFilters(
        join,
        aboveFilters,
        joinType,
        false, // Let's not push into join filter
        !joinType.generatesNullsOnLeft() && !donotOptimizeLeft,
        !joinType.generatesNullsOnRight() && !donotOptimizeRight,
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // If no filter got pushed after validate, reset filterPushed flag
    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()) {
      filterPushed = false;
    }

    boolean isAntiJoin = joinType == JoinRelType.ANTI;

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    // A ON clause filter of anti-join can not be pushed down.
    if (!isAntiJoin && RelOptUtil.classifyFilters(
        join,
        joinFilters,
        joinType,
        false,
        !joinType.generatesNullsOnLeft() && !donotOptimizeLeft,
        !joinType.generatesNullsOnRight() && !donotOptimizeRight,
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if ((!filterPushed
        && joinType == join.getJoinType())
        || (joinFilters.isEmpty()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty())) {
      return;
    }

    // create Filters on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final RelNode leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
    final RelNode rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final com.google.common.collect.ImmutableList<RelDataType> fieldTypes =
        com.google.common.collect.ImmutableList.<RelDataType>builder()
            .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
            .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder,
            RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinType == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getTraitSet(),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    relBuilder.push(newJoinRel);

    // Create a project on top of the join if some of the columns have become
    // NOT NULL due to the join-type getting stricter.
    relBuilder.convert(join.getRowType(), false);

    // create a FilterRel on top of the join if needed
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, aboveFilters,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

    call.transformTo(relBuilder.build());
  }

  /** Rule that tries to push the stream side of the filter expressions into the input of the join. */
  public static class SamzaSqlFilterIntoRemoteJoinRule extends SamzaSqlFilterRemoteJoinRule {
    public SamzaSqlFilterIntoRemoteJoinRule(boolean smart,
        RelBuilderFactory relBuilderFactory, Map<String, SqlIOConfig> systemStreamConfigBySource) {
      super(
          operand(Filter.class,
              operand(Join.class, RelOptRule.any())),
          "SamzaSqlFilterRemoteJoinRule:filter", smart, relBuilderFactory, systemStreamConfigBySource);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      perform(call, filter, join);
    }
  }
}

// End SamzaSqlFilterRemoteJoinRule.java
