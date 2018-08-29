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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.serializers.Serde;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link JoinTranslator}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LogicalJoin.class, EnumerableTableScan.class})
public class TestJoinTranslator extends TranslatorTestBase {

  @Test
  public void testTranslateStreamToTableJoin() throws IOException, ClassNotFoundException {
    // setup mock values to the constructor of FilterTranslator
    LogicalJoin mockJoin = PowerMockito.mock(LogicalJoin.class);
    TranslatorContext mockContext = mock(TranslatorContext.class);
    RelNode mockLeftInput = PowerMockito.mock(EnumerableTableScan.class);
    RelNode mockRightInput = mock(RelNode.class);
    List<RelNode> inputs = new ArrayList<>();
    inputs.add(mockLeftInput);
    inputs.add(mockRightInput);
    RelOptTable mockLeftTable = mock(RelOptTable.class);
    when(mockLeftInput.getTable()).thenReturn(mockLeftTable);
    List<String> qualifiedTableName = new ArrayList<String>() {{
      this.add("test");
      this.add("LeftTable");
    }};
    when(mockLeftTable.getQualifiedName()).thenReturn(qualifiedTableName);
    when(mockLeftInput.getId()).thenReturn(1);
    when(mockRightInput.getId()).thenReturn(2);
    when(mockJoin.getId()).thenReturn(3);
    when(mockJoin.getInputs()).thenReturn(inputs);
    when(mockJoin.getLeft()).thenReturn(mockLeftInput);
    when(mockJoin.getRight()).thenReturn(mockRightInput);
    RexCall mockJoinCondition = mock(RexCall.class);
    when(mockJoinCondition.isAlwaysTrue()).thenReturn(false);
    when(mockJoinCondition.getKind()).thenReturn(SqlKind.EQUALS);
    when(mockJoin.getCondition()).thenReturn(mockJoinCondition);
    RexInputRef mockLeftConditionInput = mock(RexInputRef.class);
    RexInputRef mockRightConditionInput = mock(RexInputRef.class);
    when(mockLeftConditionInput.getIndex()).thenReturn(0);
    when(mockRightConditionInput.getIndex()).thenReturn(0);
    List<RexNode> condOperands = new ArrayList<>();
    condOperands.add(mockLeftConditionInput);
    condOperands.add(mockRightConditionInput);
    when(mockJoinCondition.getOperands()).thenReturn(condOperands);
    RelDataType mockLeftCondDataType = mock(RelDataType.class);
    RelDataType mockRightCondDataType = mock(RelDataType.class);
    when(mockLeftCondDataType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);
    when(mockRightCondDataType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);
    when(mockLeftConditionInput.getType()).thenReturn(mockLeftCondDataType);
    when(mockRightConditionInput.getType()).thenReturn(mockRightCondDataType);
    RelDataType mockLeftRowType = mock(RelDataType.class);
    when(mockLeftRowType.getFieldCount()).thenReturn(0); //?? why ??

    when(mockLeftInput.getRowType()).thenReturn(mockLeftRowType);
    List<String> leftFieldNames = new ArrayList<String>() {{
      this.add("test_table_field1");
    }};
    List<String> rightStreamFieldNames = new ArrayList<String>() {
      {
        this.add("test_stream_field1");
      } };
    when(mockLeftRowType.getFieldNames()).thenReturn(leftFieldNames);
    RelDataType mockRightRowType = mock(RelDataType.class);
    when(mockRightInput.getRowType()).thenReturn(mockRightRowType);
    when(mockRightRowType.getFieldNames()).thenReturn(rightStreamFieldNames);

    StreamGraphSpec mockGraph = mock(StreamGraphSpec.class);
    OperatorSpec<Object, SamzaSqlRelMessage> mockLeftInputOp = mock(OperatorSpec.class);
    MessageStream<SamzaSqlRelMessage> mockLeftInputStream = new MessageStreamImpl<>(mockGraph, mockLeftInputOp);
    when(mockContext.getMessageStream(eq(mockLeftInput.getId()))).thenReturn(mockLeftInputStream);
    OperatorSpec<Object, SamzaSqlRelMessage> mockRightInputOp = mock(OperatorSpec.class);
    MessageStream<SamzaSqlRelMessage> mockRightInputStream = new MessageStreamImpl<>(mockGraph, mockRightInputOp);
    when(mockContext.getMessageStream(eq(mockRightInput.getId()))).thenReturn(mockRightInputStream);
    when(mockContext.getStreamGraph()).thenReturn(mockGraph);

    InputOperatorSpec mockInputOp = mock(InputOperatorSpec.class);
    OutputStreamImpl mockOutputStream = mock(OutputStreamImpl.class);
    when(mockInputOp.isKeyed()).thenReturn(true);
    when(mockOutputStream.isKeyed()).thenReturn(true);
    IntermediateMessageStreamImpl
        mockPartitionedStream = new IntermediateMessageStreamImpl(mockGraph, mockInputOp, mockOutputStream);
    when(mockGraph.getIntermediateStream(any(String.class), any(Serde.class), eq(false))).thenReturn(mockPartitionedStream);

    doAnswer(this.getRegisterMessageStreamAnswer()).when(mockContext).registerMessageStream(eq(3), any(MessageStream.class));
    RexToJavaCompiler mockCompiler = mock(RexToJavaCompiler.class);
    when(mockContext.getExpressionCompiler()).thenReturn(mockCompiler);
    Expression mockExpr = mock(Expression.class);
    when(mockCompiler.compile(any(), any())).thenReturn(mockExpr);

    doAnswer(this.getRegisteredTableAnswer()).when(mockGraph).getTable(any(RocksDbTableDescriptor.class));
    when(mockJoin.getJoinType()).thenReturn(JoinRelType.INNER);
    SqlIOResolver mockResolver = mock(SqlIOResolver.class);
    SqlIOConfig mockIOConfig = mock(SqlIOConfig.class);
    TableDescriptor mockTableDesc = mock(TableDescriptor.class);
    when(mockResolver.fetchSourceInfo(String.join(".", qualifiedTableName))).thenReturn(mockIOConfig);
    when(mockIOConfig.getTableDescriptor()).thenReturn(Optional.of(mockTableDesc));

    // Apply translate() method to verify that we are getting the correct map operator constructed
    JoinTranslator joinTranslator = new JoinTranslator(3, mockResolver);
    joinTranslator.translate(mockJoin, mockContext);
    // make sure that context has been registered with LogicFilter and output message streams
    verify(mockContext, times(1)).registerMessageStream(3, this.getRegisteredMessageStream(3));
    when(mockContext.getRelNode(3)).thenReturn(mockJoin);
    when(mockContext.getMessageStream(3)).thenReturn(this.getRegisteredMessageStream(3));
    StreamTableJoinOperatorSpec
        joinSpec = (StreamTableJoinOperatorSpec) Whitebox.getInternalState(this.getRegisteredMessageStream(3), "operatorSpec");
    assertNotNull(joinSpec);
    assertEquals(joinSpec.getOpCode(), OperatorSpec.OpCode.JOIN);

    // Verify joinSpec has the corresponding setup
    StreamTableJoinFunction joinFn = joinSpec.getJoinFn();
    assertNotNull(joinFn);
    assertTrue(Whitebox.getInternalState(joinFn, "isTablePosOnRight").equals(false));
    assertEquals(new ArrayList<Integer>() {{ this.add(0); }}, Whitebox.getInternalState(joinFn, "streamFieldIds"));
    assertEquals(leftFieldNames, Whitebox.getInternalState(joinFn, "tableFieldNames"));
    List<String> outputFieldNames = new ArrayList<>();
    outputFieldNames.addAll(leftFieldNames);
    outputFieldNames.addAll(rightStreamFieldNames);
    assertEquals(outputFieldNames, Whitebox.getInternalState(joinFn, "outFieldNames"));

  }

}
