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

import java.util.ArrayList;
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
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.sql.data.SamzaSqlCompositeKey;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.serializers.SamzaSqlCompositeKeySerdeFactory;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlCompositeKey.*;
import static org.apache.samza.sql.serializers.SamzaSqlCompositeKeySerdeFactory.*;
import static org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory.*;


/**
 * Translator to translate the LogicalJoin node in the relational graph to the corresponding StreamGraph
 * implementation.
 * Join is supported with the following caveats:
 *   1. Only stream-table joins are supported. No stream-stream joins.
 *   2. Only Equi-joins are supported. No theta-joins.
 *   3. Inner joins, Left and Right outer joins are supported. No cross joins, full outer joins or natural joins.
 *   4. Compound join condition with only AND operator is supported. AND operator with a literal is not supported. No
 *      support for OR operator or any other operator in the join condition.
 *
 * It is assumed that the stream denoted as 'table' is already partitioned by the key(s) specified in the join
 * condition. We do not repartition the table. But we always repartition the stream by the key(s) specified
 * in the join condition.
 */
public class JoinTranslator {

  private static final Logger log = LoggerFactory.getLogger(JoinTranslator.class);
  private int joinId;
  private SourceResolver sourceResolver;

  public JoinTranslator(int joinId, SourceResolver sourceResolver) {
    this.joinId = joinId;
    this.sourceResolver = sourceResolver;
  }

  public void translate(final LogicalJoin join, final TranslatorContext context) {

    // Do the validation of join query
    validateJoinQuery(join);

    boolean isTablePosOnRight = isTable(join.getRight());
    List<Integer> streamIds = new LinkedList<>();
    List<Integer> tableIds = new LinkedList<>();

    // Get the stream and table indices corresponding to the fields given in the join condition.
    getStreamAndTableKeyIds(((RexCall) join.getCondition()).getOperands(), join, isTablePosOnRight, streamIds, tableIds);

    MessageStream<SamzaSqlRelMessage> inputTable =
        isTablePosOnRight ?
            context.getMessageStream(join.getRight().getId()) : context.getMessageStream(join.getLeft().getId());

    SamzaSqlCompositeKeySerde keySerde =
        (SamzaSqlCompositeKeySerde) new SamzaSqlCompositeKeySerdeFactory().getSerde(null, null);
    SamzaSqlRelMessageSerde relMsgSerde =
        (SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);

    // Create a table backed by RocksDb store with the fields in the join condition as composite key and relational
    // message as the value. Send the messages from the input stream denoted as 'table' to the created table store.
    Table<KV<SamzaSqlCompositeKey, SamzaSqlRelMessage>> table =
        context.getStreamGraph()
            .getTable(new RocksDbTableDescriptor("table_" + joinId)
                .withSerde(KVSerde.of(keySerde, relMsgSerde)));

    inputTable
        .map(m -> new KV(createSamzaSqlCompositeKey(m, tableIds), m))
        .sendTo(table);

    List<String> tableFieldNames = (isTablePosOnRight ? join.getRight() : join.getLeft()).getRowType().getFieldNames();
    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(join.getJoinType(), isTablePosOnRight, streamIds, tableFieldNames);

    MessageStream<SamzaSqlRelMessage> inputStream =
        isTablePosOnRight ?
            context.getMessageStream(join.getLeft().getId()) : context.getMessageStream(join.getRight().getId());

    // Always re-partition the messages from the input stream by the composite key and then join the messages
    // with the table.
    MessageStream<SamzaSqlRelMessage> outputStream =
        inputStream
            .partitionBy(m -> createSamzaSqlCompositeKey(m, streamIds),
                m -> m,
                KVSerde.of(keySerde, relMsgSerde),
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

    RexNode condition = join.getCondition();

    // At root-level of join condition, the condition should be a RexCall. Eg: a.key = b.key
    if (!(condition instanceof RexCall)) {
      throw new SamzaException("SQL Query is not supported. Join condition " + condition +
          " is of type " + condition.getClass());
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

    validateJoinCondition(condition);
  }

  private void validateJoinCondition(RexNode operand) {
    if (!(operand instanceof RexCall)) {
      throw new SamzaException("Invalid join condition " + operand);
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

  // Get the stream and table indices corresponding to the fields given in the join condition by parsing through
  // the condition.
  private void getStreamAndTableKeyIds(List<RexNode> operands, final LogicalJoin join, boolean isTablePosOnRight,
      List<Integer> streamIds, List<Integer> tableIds) {

    // All non-leaf operands in the join condition should be expressions.
    if (operands.get(0) instanceof RexCall) {
      operands.forEach(operand -> {
        validateJoinCondition(operand);
        getStreamAndTableKeyIds(((RexCall) operand).getOperands(), join, isTablePosOnRight, streamIds, tableIds);
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
    // index in rightRef.
    RexInputRef leftRef = (RexInputRef) operands.get(0);
    RexInputRef rightRef = (RexInputRef) operands.get(1);
    if (leftRef.getIndex() > rightRef.getIndex()) {
      RexInputRef tmpRef = leftRef;
      leftRef = rightRef;
      rightRef = tmpRef;
    }

    // Get the table index and stream index
    int deltaIdx = rightRef.getIndex() - join.getLeft().getRowType().getFieldCount();
    streamIds.add(isTablePosOnRight ? leftRef.getIndex() : deltaIdx);
    tableIds.add(isTablePosOnRight ? deltaIdx : leftRef.getIndex());
  }

  private String dumpRelPlanForNode(RelNode relNode) {
    return RelOptUtil.dumpPlan("Rel expression: ",
        relNode, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  private boolean isTable(RelNode relNode) {
    // NOTE: Any intermediate form of a join is always a stream. Eg: For the second level join of
    // stream-table-table join, the left side of the join is join output, which we always
    // assume to be a stream. The intermediate stream won't be an instance of EnumerableTableScan.
    return relNode instanceof EnumerableTableScan &&
        sourceResolver.isTable(String.join(".", relNode.getTable().getQualifiedName()));
  }

  private final class SamzaSqlRelMessageJoinFunction implements StreamTableJoinFunction<SamzaSqlCompositeKey,
      SamzaSqlRelMessage, KV<SamzaSqlCompositeKey, SamzaSqlRelMessage>, SamzaSqlRelMessage> {

    JoinRelType joinRelType;
    boolean isTablePosOnRight;
    List<Integer> streamFieldIds;
    // Table field names are used in the outer join when the table record is not found.
    List<String> tableFieldNames;

    SamzaSqlRelMessageJoinFunction(JoinRelType joinRelType, boolean isTablePosOnRight, List<Integer> streamFieldIds,
        List<String> tableFieldNames) {
      this.joinRelType = joinRelType;
      this.isTablePosOnRight = isTablePosOnRight;
      Validate.isTrue((joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnRight) ||
          (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && !isTablePosOnRight) ||
          joinRelType.compareTo(JoinRelType.INNER) == 0);
      this.streamFieldIds = streamFieldIds;
      this.tableFieldNames = tableFieldNames;
    }

    @Override
    public SamzaSqlRelMessage apply(SamzaSqlRelMessage message, KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record) {

      if (joinRelType.compareTo(JoinRelType.INNER) == 0 && record == null) {
        log.debug("Record not found for the message with key: " + getMessageKey(message));
        return null;
      }

      // The resulting join output should be a SamzaSqlRelMessage containing the fields from both the stream message and
      // table record. The order of stream message fields and table record fields are dictated by the sql query. The
      // output should also include the keys from both the stream message and the table record.
      List<String> outFieldNames = new ArrayList<>();
      List<Object> outFieldValues = new ArrayList<>();

      // If table position is on the right, add the stream message fields first
      if (isTablePosOnRight) {
        outFieldNames.addAll(message.getFieldNames());
        outFieldValues.addAll(message.getFieldValues());
      }

      // Add the table record fields.
      if (record != null) {
        outFieldNames.addAll(record.getValue().getFieldNames());
        outFieldValues.addAll(record.getValue().getFieldValues());
      } else {
        // Table record could be null as the record could not be found in the store. This can
        // happen for outer joins. Add nulls to all the field values in the output message.
        outFieldNames.addAll(tableFieldNames);
        tableFieldNames.forEach(s -> outFieldValues.add(null));
      }

      // If table position is on the left, add the stream message fields last
      if (!isTablePosOnRight) {
        outFieldNames.addAll(message.getFieldNames());
        outFieldValues.addAll(message.getFieldValues());
      }

      return new SamzaSqlRelMessage(outFieldNames, outFieldValues);
    }

    @Override
    public SamzaSqlCompositeKey getMessageKey(SamzaSqlRelMessage message) {
      return createSamzaSqlCompositeKey(message, streamFieldIds);
    }

    @Override
    public SamzaSqlCompositeKey getRecordKey(KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record) {
      return record.getKey();
    }

    @Override
    public void close() {
    }
  }
}
