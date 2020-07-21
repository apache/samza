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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.Validate;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.data.SamzaSqlRelMessage.createSamzaSqlCompositeKey;
import static org.apache.samza.sql.data.SamzaSqlRelMessage.getSamzaSqlCompositeKeyFieldNames;


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
  private final TranslatorInputMetricsMapFunction inputMetricsMF;
  private final TranslatorOutputMetricsMapFunction outputMetricsMF;

  JoinTranslator(String logicalOpId, String intermediateStreamPrefix, int queryId) {
    this.logicalOpId = logicalOpId;
    this.intermediateStreamPrefix = intermediateStreamPrefix + (intermediateStreamPrefix.isEmpty() ? "" : "_");
    this.queryId = queryId;
    this.inputMetricsMF = new TranslatorInputMetricsMapFunction(logicalOpId);
    this.outputMetricsMF = new TranslatorOutputMetricsMapFunction(logicalOpId);
  }

  void translate(final LogicalJoin join, final TranslatorContext translatorContext) {
    JoinInputNode.InputType inputTypeOnLeft = JoinInputNode.getInputType(join.getLeft(),
        translatorContext.getExecutionContext().getSamzaSqlApplicationConfig().getInputSystemStreamConfigBySource());
    JoinInputNode.InputType inputTypeOnRight = JoinInputNode.getInputType(join.getRight(),
        translatorContext.getExecutionContext().getSamzaSqlApplicationConfig().getInputSystemStreamConfigBySource());

    // Do the validation of join query
    validateJoinQuery(join, inputTypeOnLeft, inputTypeOnRight);

    // At this point, one of the sides is a table. Let's figure out if it is on left or right side.
    boolean isTablePosOnRight = inputTypeOnRight != JoinInputNode.InputType.STREAM;

    // stream and table keyIds are used to extract the join condition field (key) names and values out of the stream
    // and table records.
    List<Integer> streamKeyIds = new LinkedList<>();
    List<Integer> tableKeyIds = new LinkedList<>();

    // Fetch the stream and table indices corresponding to the fields given in the join condition.

    final int leftSideSize = join.getLeft().getRowType().getFieldCount();
    final int tableStartIdx = isTablePosOnRight ? leftSideSize : 0;
    final int streamStartIdx = isTablePosOnRight ? 0 : leftSideSize;
    final int tableEndIdx = isTablePosOnRight ? join.getRowType().getFieldCount() : leftSideSize;
    join.getCondition().accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        validateJoinKeyType(inputRef); // Validate the type of the input ref.
        int index = inputRef.getIndex();
        if (index >= tableStartIdx && index < tableEndIdx) {
          tableKeyIds.add(index - tableStartIdx);
        } else {
          streamKeyIds.add(index - streamStartIdx);
        }
        return inputRef;
      }
    });
    Collections.sort(tableKeyIds);
    Collections.sort(streamKeyIds);

    // Get the two input nodes (stream and table nodes) for the join.
    JoinInputNode streamNode = new JoinInputNode(isTablePosOnRight ? join.getLeft() : join.getRight(), streamKeyIds,
        isTablePosOnRight ? inputTypeOnLeft : inputTypeOnRight, !isTablePosOnRight);
    JoinInputNode tableNode = new JoinInputNode(isTablePosOnRight ? join.getRight() : join.getLeft(), tableKeyIds,
        isTablePosOnRight ? inputTypeOnRight : inputTypeOnLeft, isTablePosOnRight);

    MessageStream<SamzaSqlRelMessage> inputStream = translatorContext.getMessageStream(streamNode.getRelNode().getId());
    Table table = getTable(tableNode, translatorContext);

    MessageStream<SamzaSqlRelMessage> outputStream =
        joinStreamWithTable(inputStream, table, streamNode, tableNode, join, translatorContext);

    translatorContext.registerMessageStream(join.getId(), outputStream);

    outputStream.map(outputMetricsMF);
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

      return inputStream
          .map(inputMetricsMF)
          .join(table, joinFn);
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
            .map(inputMetricsMF)
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

    boolean isTablePosOnLeft = inputTypeOnLeft != JoinInputNode.InputType.STREAM;
    boolean isTablePosOnRight = inputTypeOnRight != JoinInputNode.InputType.STREAM;

    if (!isTablePosOnLeft && !isTablePosOnRight) {
      throw new SamzaException("Invalid query with both sides of join being denoted as 'stream'. "
          + "Stream-stream join is not yet supported. " + dumpRelPlanForNode(join));
    }

    if (isTablePosOnLeft && isTablePosOnRight) {
      throw new SamzaException("Invalid query with both sides of join being denoted as 'table'. " +
          dumpRelPlanForNode(join));
    }

    if (joinRelType.compareTo(JoinRelType.LEFT) == 0 && isTablePosOnLeft) {
      throw new SamzaException("Invalid query for outer left join. Left side of the join should be a 'stream' and "
          + "right side of join should be a 'table'. " + dumpRelPlanForNode(join));
    }

    if (joinRelType.compareTo(JoinRelType.RIGHT) == 0 && isTablePosOnRight) {
      throw new SamzaException("Invalid query for outer right join. Left side of the join should be a 'table' and "
          + "right side of join should be a 'stream'. " + dumpRelPlanForNode(join));
    }

    final List<RexNode> conjunctionList = new ArrayList<>();
    decomposeAndValidateConjunction(join.getCondition(), conjunctionList);

    if (conjunctionList.isEmpty()) {
      throw new SamzaException("Query results in a cross join, which is not supported. Please optimize the query."
          + " It is expected that the joins should include JOIN ON operator in the sql query.");
    }
    //TODO Not sure why we can not allow literal as part of the join condition will revisit this in another scope
    conjunctionList.forEach(rexNode -> rexNode.accept(new RexShuttle() {
      @Override
      public RexNode visitLiteral(RexLiteral literal) {
        throw new SamzaException(
            "Join Condition can not allow literal " + literal.toString() + " join node" + join.getDigest());
      }
    }));
    final JoinInputNode.InputType rootTableInput = isTablePosOnRight ? inputTypeOnRight : inputTypeOnLeft;
    if (rootTableInput.compareTo(JoinInputNode.InputType.REMOTE_TABLE) != 0) {
      // it is not a remote table all is good we do not have to validate the project on key Column
      return;
    }

    /*
    For remote Table we need to validate The join Condition and The project that is above remote table scan.
     - As of today Filter need to be exactly one equi-join using the __key__ column (see SAMZA-2554)
     - The Project on the top of the remote table has to contain only simple input references to any of the column used in the join.
    */

    // First let's collect the ref of columns used by the join condition.
    List<RexInputRef> refCollector = new ArrayList<>();
    join.getCondition().accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        refCollector.add(inputRef);
        return inputRef;
      }
    });
    // start index of the Remote table within the Join Row
    final int tableStartIndex = isTablePosOnRight ? join.getLeft().getRowType().getFieldCount() : 0;
    // end index of the Remote table withing the Join Row
    final int tableEndIndex =
        isTablePosOnRight ? join.getRowType().getFieldCount() : join.getLeft().getRowType().getFieldCount();

    List<Integer> tableRefsIdx = refCollector.stream()
        .map(x -> x.getIndex())
        .filter(x -> tableStartIndex <= x && x < tableEndIndex) // collect all the refs form table side
        .map(x -> x - tableStartIndex) // re-adjust the offset
        .sorted()
        .collect(Collectors.toList()); // we have a list with all the input from table side with 0 based index.

    // Validate the Condition must contain a ref to remote table primary key column.

    if (conjunctionList.size() != 1 || tableRefsIdx.size() != 1) {
      //TODO We can relax this by allowing another filter to be evaluated post lookup see SAMZA-2554
      throw new SamzaException(
          "Invalid query for join condition must contain exactly one predicate for remote table on __key__ column "
              + dumpRelPlanForNode(join));
    }

    // Validate the Project, follow each input and ensure that it is a simple ref with no rexCall in the way.
    if (!isValidRemoteJoinRef(tableRefsIdx.get(0), isTablePosOnRight ? join.getRight() : join.getLeft())) {
      throw new SamzaException("Invalid query for join condition can not have an expression and must be reference "
          + SamzaSqlRelMessage.KEY_NAME + " column " + dumpRelPlanForNode(join));
    }
  }

  /**
   * Helper method to check if the join condition can be evaluated by the remote table.
   * It does follow single path  using the index ref path checking if it is a simple reference all the way to table scan.
   * In case any RexCall is encountered will stop an return null as a marker otherwise will return Column Name.
   *
   * @param inputRexIndex rex ref index
   * @param relNode current Rel Node
   * @return false if any Relational Expression is encountered on the path, true if is simple ref to __key__ column.
   */
  private static boolean isValidRemoteJoinRef(int inputRexIndex, RelNode relNode) {
    if (relNode instanceof TableScan) {
      return relNode.getRowType().getFieldList().get(inputRexIndex).getName().equals(SamzaSqlRelMessage.KEY_NAME);
    }
    // has to be a single rel kind filter/project/table scan
    Preconditions.checkState(relNode.getInputs().size() == 1,
        "Has to be single input RelNode and got " + relNode.getDigest());
    if (relNode instanceof LogicalFilter) {
      return isValidRemoteJoinRef(inputRexIndex, relNode.getInput(0));
    }
    RexNode inputRef = ((LogicalProject) relNode).getProjects().get(inputRexIndex);
    if (inputRef instanceof RexCall) {
      return false; // we can not push any expression as of now stop and return null.
    }
    return isValidRemoteJoinRef(((RexInputRef) inputRef).getIndex(), relNode.getInput(0));
  }



  /**
   * Traverse the tree of expression and validate. Only allowed predicate is conjunction of exp1 = exp2
   * @param rexPredicate Rex Condition
   * @param conjunctionList result container to pull result form recursion stack.
   */
  public static void decomposeAndValidateConjunction(RexNode rexPredicate, List<RexNode> conjunctionList) {
    if (rexPredicate == null || rexPredicate.isAlwaysTrue()) {
      return;
    }

    if (rexPredicate.isA(SqlKind.AND)) {
      for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
        decomposeAndValidateConjunction(operand, conjunctionList);
      }
    } else if (rexPredicate.isA(SqlKind.EQUALS)) {
      conjunctionList.add(rexPredicate);
    } else {
      throw new SamzaException("Only equi-joins and AND operator is supported in join condition.");
    }
  }

  private void validateJoinKeyType(RexInputRef ref) {
    SqlTypeName sqlTypeName = ref.getType().getSqlTypeName();

    // Primitive types and ANY (for the record key) are supported in the key
    if (sqlTypeName != SqlTypeName.BOOLEAN && sqlTypeName != SqlTypeName.TINYINT && sqlTypeName != SqlTypeName.SMALLINT
        && sqlTypeName != SqlTypeName.INTEGER && sqlTypeName != SqlTypeName.CHAR && sqlTypeName != SqlTypeName.BIGINT
        && sqlTypeName != SqlTypeName.VARCHAR && sqlTypeName != SqlTypeName.DOUBLE && sqlTypeName != SqlTypeName.FLOAT
        && sqlTypeName != SqlTypeName.ANY && sqlTypeName != SqlTypeName.OTHER) {
      log.error("Unsupported key type " + sqlTypeName + " used in join condition.");
      throw new SamzaException("Unsupported key type used in join condition.");
    }
  }

  private String dumpRelPlanForNode(RelNode relNode) {
    return RelOptUtil.dumpPlan("Rel expression: ",
        relNode, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  static SqlIOConfig resolveSQlIOForTable(RelNode relNode, Map<String, SqlIOConfig> systemStreamConfigBySource) {
    // Let's recursively get to the TableScan node to identify IO for the table.

    if (relNode instanceof HepRelVertex) {
      return resolveSQlIOForTable(((HepRelVertex) relNode).getCurrentRel(), systemStreamConfigBySource);
    }

    if (relNode instanceof LogicalProject) {
      return resolveSQlIOForTable(((LogicalProject) relNode).getInput(), systemStreamConfigBySource);
    }

    if (relNode instanceof LogicalFilter) {
      return resolveSQlIOForTable(((LogicalFilter) relNode).getInput(), systemStreamConfigBySource);
    }

    // We return null for table IO as the table seems to be involved in another join. The output of stream-table join
    // is considered a stream. Hence, we return null for the table.
    if (relNode instanceof LogicalJoin && relNode.getInputs().size() > 1) {
      return null;
    }

    if (!(relNode instanceof TableScan)) {
      throw new SamzaException(String.format("Unsupported query. relNode %s is not of type TableScan.",
          relNode.toString()));
    }

    String sourceName = SqlIOConfig.getSourceFromSourceParts(relNode.getTable().getQualifiedName());
    SqlIOConfig sourceConfig = systemStreamConfigBySource.get(sourceName);
    if (sourceConfig == null) {
      throw new SamzaException("Unsupported source found in join statement: " + sourceName);
    }
    return sourceConfig;
  }

  private Table getTable(JoinInputNode tableNode, TranslatorContext context) {

    SqlIOConfig sourceTableConfig = resolveSQlIOForTable(tableNode.getRelNode(),
        context.getExecutionContext().getSamzaSqlApplicationConfig().getInputSystemStreamConfigBySource());

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

  @VisibleForTesting
  public TranslatorInputMetricsMapFunction getInputMetricsMF() {
    return this.inputMetricsMF;
  }

  @VisibleForTesting
  public TranslatorOutputMetricsMapFunction getOutputMetricsMF() {
    return this.outputMetricsMF;
  }

}
