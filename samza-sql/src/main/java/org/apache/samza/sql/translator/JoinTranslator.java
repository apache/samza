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
import org.apache.calcite.rel.logical.LogicalProject;
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
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlRelMessage.getSamzaSqlCompositeKeyFieldNames;
import static org.apache.samza.sql.data.SamzaSqlRelMessage.createSamzaSqlCompositeKey;


/**
 * Translator to translate the LogicalJoin node in the relational graph to the corresponding StreamGraph
 * implementation.
 * Join is supported with the following caveats:
 *   1. Only stream-table joins are supported. No stream-stream joins.
 *   2. Only Equi-joins are supported. No theta-joins.
 *   3. Inner joins, Left and Right outer joins are supported. No cross joins, full outer joins or natural joins.
 *   4. Join condition with a constant is not supported.
 *   5. Compound join condition with only AND operator is supported. AND operator with a constant is not supported. No
 *      support for OR operator or any other operator in the join condition.
 * For local table, we always repartition both the stream to be joined and the stream denoted as table by the key(s)
 * specified in the join condition.
 */
class JoinTranslator {

  private static final Logger log = LoggerFactory.getLogger(JoinTranslator.class);
  private String logicalOpId;
  private final String intermediateStreamPrefix;
  private final int queryId;

  JoinTranslator(String logicalOpId, String intermediateStreamPrefix, int queryId) {
    this.logicalOpId = logicalOpId;
    this.intermediateStreamPrefix = intermediateStreamPrefix + (intermediateStreamPrefix.isEmpty() ? "" : "_");
    this.queryId = queryId;
  }

  void translate(final LogicalJoin join, final TranslatorContext context) {
    JoinInputNode.InputType inputTypeOnLeft = getInputType(join.getLeft(), context);
    JoinInputNode.InputType inputTypeOnRight = getInputType(join.getRight(), context);

    // Do the validation of join query
    validateJoinQuery(join, inputTypeOnLeft, inputTypeOnRight);

    // At this point, one of the sides is a table. Let's figure out if it is on left or right side.
    boolean isTablePosOnRight = (inputTypeOnRight != JoinInputNode.InputType.STREAM);

    // stream and table keyIds are used to extract the join condition field (key) names and values out of the stream
    // and table records.
    List<Integer> streamKeyIds = new LinkedList<>();
    List<Integer> tableKeyIds = new LinkedList<>();

    // Fetch the stream and table indices corresponding to the fields given in the join condition.
    populateStreamAndTableKeyIds(((RexCall) join.getCondition()).getOperands(), join, isTablePosOnRight, streamKeyIds,
        tableKeyIds);

    // Get the two input nodes (stream and table nodes) for the join.
    JoinInputNode streamNode = new JoinInputNode(isTablePosOnRight ? join.getLeft() : join.getRight(), streamKeyIds,
        isTablePosOnRight ? inputTypeOnLeft : inputTypeOnRight, !isTablePosOnRight);
    JoinInputNode tableNode = new JoinInputNode(isTablePosOnRight ? join.getRight() : join.getLeft(), tableKeyIds,
        isTablePosOnRight ? inputTypeOnRight : inputTypeOnLeft, isTablePosOnRight);

    MessageStream<SamzaSqlRelMessage> inputStream = context.getMessageStream(streamNode.getRelNode().getId());
    Table table = getTable(tableNode, context);

    MessageStream<SamzaSqlRelMessage> outputStream =
        joinStreamWithTable(inputStream, table, streamNode, tableNode, join, context);

    context.registerMessageStream(join.getId(), outputStream);
  }

  private MessageStream<SamzaSqlRelMessage> joinStreamWithTable(MessageStream<SamzaSqlRelMessage> inputStream,
      Table table, JoinInputNode streamNode, JoinInputNode tableNode, LogicalJoin join, TranslatorContext context) {

    List<Integer> streamKeyIds = streamNode.getKeyIds();
    List<Integer> tableKeyIds = tableNode.getKeyIds();
    Validate.isTrue(streamKeyIds.size() == tableKeyIds.size());

    log.info("Joining on the following Stream and Table field(s): ");
    List<String> streamFieldNames = new ArrayList<>(streamNode.getFieldNames());
    List<String> tableFieldNames = new ArrayList<>(tableNode.getFieldNames());
    for (int i = 0; i < streamKeyIds.size(); i++) {
      log.info(streamFieldNames.get(streamKeyIds.get(i)) + " with " + tableFieldNames.get(tableKeyIds.get(i)));
    }

    if (tableNode.isRemoteTable()) {
      String remoteTableName = tableNode.getSourceName();
      StreamTableJoinFunction joinFn = new SamzaSqlRemoteTableJoinFunction(context.getMsgConverter(remoteTableName),
          context.getTableKeyConverter(remoteTableName), streamNode, tableNode, join.getJoinType(), queryId);

      return inputStream.join(table, joinFn);
    }

    // Join with the local table

    StreamTableJoinFunction joinFn = new SamzaSqlLocalTableJoinFunction(streamNode, tableNode, join.getJoinType());

    SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde keySerde =
        (SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
    SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde valueSerde =
        (SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);

    // Always re-partition the messages from the input stream by the composite key and then join the messages
    // with the table. For the composite key, provide the corresponding table names in the key instead of using
    // the names from the stream as the lookup needs to be done based on what is stored in the local table.
    return
        inputStream
            .partitionBy(m -> createSamzaSqlCompositeKey(m, streamKeyIds,
            getSamzaSqlCompositeKeyFieldNames(tableFieldNames, tableKeyIds)), m -> m, KVSerde.of(keySerde, valueSerde),
            intermediateStreamPrefix + "stream_" + logicalOpId)
            .map(KV::getValue)
            .join(table, joinFn);
  }

  private void validateJoinQuery(LogicalJoin join, JoinInputNode.InputType inputTypeOnLeft,
      JoinInputNode.InputType inputTypeOnRight) {
    JoinRelType joinRelType = join.getJoinType();

    if (joinRelType.compareTo(JoinRelType.INNER) != 0 && joinRelType.compareTo(JoinRelType.LEFT) != 0
        && joinRelType.compareTo(JoinRelType.RIGHT) != 0) {
      throw new SamzaException("Query with only INNER and LEFT/RIGHT OUTER join are supported.");
    }

    boolean isTablePosOnLeft = (inputTypeOnLeft != JoinInputNode.InputType.STREAM);
    boolean isTablePosOnRight = (inputTypeOnRight != JoinInputNode.InputType.STREAM);

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
    validateJoinKeys(leftRef);
    validateJoinKeys(rightRef);

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

  private void validateJoinKeys(RexInputRef ref) {
    SqlTypeName sqlTypeName = ref.getType().getSqlTypeName();

    // Primitive types and ANY (for the record key) are supported in the key
    if (sqlTypeName != SqlTypeName.BOOLEAN && sqlTypeName != SqlTypeName.TINYINT && sqlTypeName != SqlTypeName.SMALLINT
        && sqlTypeName != SqlTypeName.INTEGER && sqlTypeName != SqlTypeName.CHAR && sqlTypeName != SqlTypeName.BIGINT
        && sqlTypeName != SqlTypeName.VARCHAR && sqlTypeName != SqlTypeName.DOUBLE && sqlTypeName != SqlTypeName.FLOAT
        && sqlTypeName != SqlTypeName.ANY) {
      log.error("Unsupported key type " + sqlTypeName + " used in join condition.");
      throw new SamzaException("Unsupported key type used in join condition.");
    }
  }

  private String dumpRelPlanForNode(RelNode relNode) {
    return RelOptUtil.dumpPlan("Rel expression: ",
        relNode, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  private SqlIOConfig resolveSourceConfigForTable(RelNode relNode, TranslatorContext context) {
    if (relNode instanceof LogicalProject) {
      return resolveSourceConfigForTable(((LogicalProject) relNode).getInput(), context);
    }

    // We are returning the sourceConfig for the table as null when the table is in another join rather than an output
    // table, that's because the output of stream-table join is considered a stream.
    if (relNode.getInputs().size() > 1) {
      return null;
    }

    String sourceName = SqlIOConfig.getSourceFromSourceParts(relNode.getTable().getQualifiedName());
    SqlIOConfig sourceConfig =
        context.getExecutionContext().getSamzaSqlApplicationConfig().getInputSystemStreamConfigBySource().get(sourceName);
    if (sourceConfig == null) {
      throw new SamzaException("Unsupported source found in join statement: " + sourceName);
    }
    return sourceConfig;
  }

  private JoinInputNode.InputType getInputType(RelNode relNode, TranslatorContext context) {

    // NOTE: Any intermediate form of a join is always a stream. Eg: For the second level join of
    // stream-table-table join, the left side of the join is join output, which we always
    // assume to be a stream. The intermediate stream won't be an instance of EnumerableTableScan.
    // The join key(s) for the table could be an udf in which case the relNode would be LogicalProject.

    if (relNode instanceof EnumerableTableScan || relNode instanceof LogicalProject) {
      SqlIOConfig sourceTableConfig = resolveSourceConfigForTable(relNode, context);
      if (sourceTableConfig == null || !sourceTableConfig.getTableDescriptor().isPresent()) {
        return JoinInputNode.InputType.STREAM;
      } else if (sourceTableConfig.getTableDescriptor().get() instanceof RemoteTableDescriptor) {
        return JoinInputNode.InputType.REMOTE_TABLE;
      } else {
        return JoinInputNode.InputType.LOCAL_TABLE;
      }
    } else {
      return JoinInputNode.InputType.STREAM;
    }
  }

  private Table getTable(JoinInputNode tableNode, TranslatorContext context) {

    SqlIOConfig sourceTableConfig = resolveSourceConfigForTable(tableNode.getRelNode(), context);

    if (sourceTableConfig == null || !sourceTableConfig.getTableDescriptor().isPresent()) {
      String errMsg = "Failed to resolve table source in join operation: node=" + tableNode.getRelNode();
      log.error(errMsg);
      throw new SamzaException(errMsg);
    }

    Table<KV<SamzaSqlRelRecord, SamzaSqlRelMessage>> table =
        context.getStreamAppDescriptor().getTable(sourceTableConfig.getTableDescriptor().get());

    if (tableNode.isRemoteTable()) {
      return table;
    }

    // If local table, load the table.

    // Load the local table with the fields in the join condition as composite key and relational message as the value.
    // Send the messages from the input stream denoted as 'table' to the created table store.

    MessageStream<SamzaSqlRelMessage> relOutputStream = context.getMessageStream(tableNode.getRelNode().getId());

    SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde keySerde =
        (SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
    SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde valueSerde =
        (SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);

    List<Integer> tableKeyIds = tableNode.getKeyIds();

    // Let's always repartition by the join fields as key before sending the key and value to the table.
    // We need to repartition the stream denoted as table to ensure that both the stream and table that are joined
    // have the same partitioning scheme with the same partition key and number. Please note that bootstrap semantic is
    // not propagated to the intermediate streams. Please refer SAMZA-1613 for more details on this. Subsequently, the
    // results are consistent only after the local table is caught up.

    relOutputStream
        .partitionBy(m -> createSamzaSqlCompositeKey(m, tableKeyIds), m -> m,
            KVSerde.of(keySerde, valueSerde), intermediateStreamPrefix + "table_" + logicalOpId)
        .sendTo(table);

    return table;
  }
}
