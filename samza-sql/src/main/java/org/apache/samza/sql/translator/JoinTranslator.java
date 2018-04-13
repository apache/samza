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

package org.apache.samza.sql.translator;

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.sql.data.SamzaSqlCompositeKey;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.impl.TableJoinUtils;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
import org.apache.samza.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlCompositeKey.createSamzaSqlCompositeKey;
import static org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde;


/**
 * Translator to translate the LogicalJoin node in the relational graph to the corresponding StreamGraph
 * implementation.
 * Join is supported with the following caveats:
 *   0. Only local tables are supported. Remote/composite tables are not yet supported.
 *   1. Only stream-table joins are supported. No stream-stream joins.
 *   2. Only Equi-joins are supported. No theta-joins.
 *   3. Inner joins, Left and Right outer joins are supported. No cross joins, full outer joins or natural joins.
 *   4. Join condition with a constant is not supported.
 *   5. Compound join condition with only AND operator is supported. AND operator with a constant is not supported. No
 *      support for OR operator or any other operator in the join condition.
 *   6. Join condition with UDFs is not supported. Eg: udf1(a.key) = udf2(b.key) is not supported.
 *
 * It is assumed that the stream denoted as 'table' is already partitioned by the key(s) specified in the join
 * condition. We do not repartition the table as bootstrap semantic is not propagated to the intermediate streams.
 * Please refer SAMZA-1613 for more details on this. But we always repartition the stream by the key(s) specified in
 * the join condition.
 */
class JoinTranslator {

  private static final Logger log = LoggerFactory.getLogger(JoinTranslator.class);
  private int joinId;
  private SourceResolver sourceResolver;

  JoinTranslator(int joinId, SourceResolver sourceResolver) {
    this.joinId = joinId;
    this.sourceResolver = sourceResolver;
  }

  void translate(final LogicalJoin join, final TranslatorContext context) {

    // Do the validation of join query
    validateJoinQuery(join);

    boolean isTablePosOnRight = isTable(join.getRight());
    List<Integer> streamKeyIds = new LinkedList<>();
    List<Integer> tableKeyIds = new LinkedList<>();

    // Fetch the stream and table indices corresponding to the fields given in the join condition.
    populateStreamAndTableKeyIds(((RexCall) join.getCondition()).getOperands(), join, isTablePosOnRight, streamKeyIds,
        tableKeyIds);

    Table table = loadLocalTable(isTablePosOnRight, tableKeyIds, join, context);

    MessageStream<SamzaSqlRelMessage> inputStream =
        isTablePosOnRight ?
            context.getMessageStream(join.getLeft().getId()) : context.getMessageStream(join.getRight().getId());

    List<String> streamFieldNames = (isTablePosOnRight ? join.getLeft() : join.getRight()).getRowType().getFieldNames();
    List<String> tableFieldNames = (isTablePosOnRight ? join.getRight() : join.getLeft()).getRowType().getFieldNames();
    Validate.isTrue(streamKeyIds.size() == tableKeyIds.size());
    log.info("Joining on the following Stream and Table field(s): ");
    for (int i = 0; i < streamKeyIds.size(); i++) {
      log.info(streamFieldNames.get(streamKeyIds.get(i)) + " with " + tableFieldNames.get(tableKeyIds.get(i)));
    }

    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(join.getJoinType(), isTablePosOnRight, streamKeyIds, streamFieldNames,
            tableFieldNames);

    // Always re-partition the messages from the input stream by the composite key and then join the messages
    // with the table.
    MessageStream<SamzaSqlRelMessage> outputStream =
        inputStream
            .partitionBy(m -> createSamzaSqlCompositeKey(m, streamKeyIds),
                m -> m,
                KVSerde.of(TableJoinUtils.getKeySerde(), TableJoinUtils.getValueSerde()),
                "stream_" + joinId)
            .map(KV::getValue)
            .join(table, joinFn);

    context.registerMessageStream(join.getId(), outputStream);
  }

  private void validateJoinQuery(LogicalJoin join) {
    JoinRelType joinRelType = join.getJoinType();

    if (joinRelType.compareTo(JoinRelType.INNER) != 0 && joinRelType.compareTo(JoinRelType.LEFT) != 0
        && joinRelType.compareTo(JoinRelType.RIGHT) != 0) {
      throw new SamzaException("Query with only INNER and LEFT/RIGHT OUTER join are supported.");
    }

    boolean isTablePosOnLeft = isTable(join.getLeft());
    boolean isTablePosOnRight = isTable(join.getRight());

    if (!isTablePosOnLeft && !isTablePosOnRight) {
      throw new SamzaException("Invalid query with both sides of join being denoted as 'stream'. "
          + "Stream-stream join is not yet supported. " + dumpRelPlanForNode(join));
    }

    if (isTablePosOnLeft && isTablePosOnRight) {
      throw new SamzaException("Invalid query with both sides of join being denoted as 'table'. " +
          dumpRelPlanForNode(join));
    }

    if (joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnLeft && !isTablePosOnRight) {
      throw new SamzaException("Invalid query for outer left join. Left side of the join should be a 'stream' and "
          + "right side of join should be a 'table'. " + dumpRelPlanForNode(join));
    }

    if (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && isTablePosOnRight && !isTablePosOnLeft) {
      throw new SamzaException("Invalid query for outer right join. Left side of the join should be a 'table' and "
          + "right side of join should be a 'stream'. " + dumpRelPlanForNode(join));
    }

    validateJoinCondition(join.getCondition());
  }

  private void validateJoinCondition(RexNode operand) {
    if (!(operand instanceof RexCall)) {
      throw new SamzaException("SQL Query is not supported. Join condition operand " + operand +
          " is of type " + operand.getClass());
    }

    RexCall condition = (RexCall) operand;

    if (condition.isAlwaysTrue()) {
      throw new SamzaException("Query results in a cross join, which is not supported. Please optimize the query."
          + " It is expected that the joins should include JOIN ON operator in the sql query.");
    }

    if (condition.getKind() != SqlKind.EQUALS && condition.getKind() != SqlKind.AND) {
      throw new SamzaException("Only equi-joins and AND operator is supported in join condition.");
    }
  }

  // Fetch the stream and table key indices corresponding to the fields given in the join condition by parsing through
  // the condition. Stream and table key indices are populated in streamKeyIds and tableKeyIds respectively.
  private void populateStreamAndTableKeyIds(List<RexNode> operands, final LogicalJoin join, boolean isTablePosOnRight,
      List<Integer> streamKeyIds, List<Integer> tableKeyIds) {

    // All non-leaf operands in the join condition should be expressions.
    if (operands.get(0) instanceof RexCall) {
      operands.forEach(operand -> {
        validateJoinCondition(operand);
        populateStreamAndTableKeyIds(((RexCall) operand).getOperands(), join, isTablePosOnRight, streamKeyIds, tableKeyIds);
      });
      return;
    }

    // We are at the leaf of the join condition. Only binary operators are supported.
    Validate.isTrue(operands.size() == 2);

    // Only reference operands are supported in row expressions and not constants.
    // a.key = b.key is supported with a.key and b.key being reference operands.
    // a.key = "constant" is not yet supported.
    if (!(operands.get(0) instanceof RexInputRef) || !(operands.get(1) instanceof RexInputRef)) {
      throw new SamzaException("SQL query is not supported. Join condition " + join.getCondition() + " should have "
          + "reference operands but the types are " + operands.get(0).getClass() + " and " + operands.get(1).getClass());
    }

    // Join condition is commutative, meaning, a.key = b.key is equivalent to b.key = a.key.
    // Calcite assigns the indices to the fields based on the order a and b are specified in
    // the sql 'from' clause. Let's put the operand with smaller index in leftRef and larger
    // index in rightRef so that the order of operands in the join condition is in the order
    // the stream and table are specified in the 'from' clause.
    RexInputRef leftRef = (RexInputRef) operands.get(0);
    RexInputRef rightRef = (RexInputRef) operands.get(1);

    // Let's validate the key used in the join condition.
    validateKey(leftRef);
    validateKey(rightRef);

    if (leftRef.getIndex() > rightRef.getIndex()) {
      RexInputRef tmpRef = leftRef;
      leftRef = rightRef;
      rightRef = tmpRef;
    }

    // Get the table key index and stream key index
    int deltaKeyIdx = rightRef.getIndex() - join.getLeft().getRowType().getFieldCount();
    streamKeyIds.add(isTablePosOnRight ? leftRef.getIndex() : deltaKeyIdx);
    tableKeyIds.add(isTablePosOnRight ? deltaKeyIdx : leftRef.getIndex());
  }

  private void validateKey(RexInputRef ref) {
    SqlTypeName sqlTypeName = ref.getType().getSqlTypeName();
    // Only primitive types are supported in the key
    if (sqlTypeName != SqlTypeName.BOOLEAN && sqlTypeName != SqlTypeName.TINYINT && sqlTypeName != SqlTypeName.SMALLINT
        && sqlTypeName != SqlTypeName.INTEGER && sqlTypeName != SqlTypeName.CHAR && sqlTypeName != SqlTypeName.BIGINT
        && sqlTypeName != SqlTypeName.VARCHAR && sqlTypeName != SqlTypeName.DOUBLE && sqlTypeName != SqlTypeName.FLOAT) {
      log.error("Unsupported key type " + sqlTypeName + " used in join condition.");
      throw new SamzaException("Unsupported key type used in join condition.");
    }
  }

  private String dumpRelPlanForNode(RelNode relNode) {
    return RelOptUtil.dumpPlan("Rel expression: ",
        relNode, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  private SqlSystemSourceConfig resolveSourceConfig(RelNode relNode) {
    String sourceName = String.join(".", relNode.getTable().getQualifiedName());
    SqlSystemSourceConfig sourceConfig = sourceResolver.fetchSourceInfo(sourceName, false);
    if (sourceConfig == null) {
      throw new SamzaException("Unsupported source found in join statement: " + sourceName);
    }
    return sourceConfig;
  }

  private boolean isTable(RelNode relNode) {
    // NOTE: Any intermediate form of a join is always a stream. Eg: For the second level join of
    // stream-table-table join, the left side of the join is join output, which we always
    // assume to be a stream. The intermediate stream won't be an instance of EnumerableTableScan.
    if (relNode instanceof EnumerableTableScan) {
      return resolveSourceConfig(relNode).isTable();
    } else {
      return false;
    }
  }

  private Table loadLocalTable(boolean isTablePosOnRight, List<Integer> tableKeyIds, LogicalJoin join, TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> relOutputStream =
        isTablePosOnRight ?
            context.getMessageStream(join.getRight().getId()) : context.getMessageStream(join.getLeft().getId());

    SqlSystemSourceConfig sourceConfig =
        isTablePosOnRight ?
            resolveSourceConfig(join.getRight()) : resolveSourceConfig(join.getLeft());

    // Create a table backed by RocksDb store with the fields in the join condition as composite key and relational
    // message as the value. Send the messages from the input stream denoted as 'table' to the created table store.
    Table<KV<SamzaSqlCompositeKey, SamzaSqlRelMessage>> table =
        context.getStreamGraph().getTable(sourceConfig.getTableDescriptor());

    relOutputStream
        .map(m -> new KV(createSamzaSqlCompositeKey(m, tableKeyIds), m))
        .sendTo(table);

    return table;
  }
}
