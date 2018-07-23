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
import java.util.HashSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FilterTranslator}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(LogicalFilter.class)
public class TestFilterTranslator extends TranslatorTestBase {

  @Test
  public void testTranslate() throws IOException, ClassNotFoundException {
    // setup mock values to the constructor of FilterTranslator
    LogicalFilter mockFilter = PowerMockito.mock(LogicalFilter.class);
    TranslatorContext mockContext = mock(TranslatorContext.class);
    RelNode mockInput = mock(RelNode.class);
    when(mockFilter.getInput()).thenReturn(mockInput);
    when(mockInput.getId()).thenReturn(1);
    when(mockFilter.getId()).thenReturn(2);
    StreamGraphSpec mockGraph = mock(StreamGraphSpec.class);
    OperatorSpec<Object, SamzaSqlRelMessage> mockInputOp = mock(OperatorSpec.class);
    MessageStream<SamzaSqlRelMessage> mockStream = new MessageStreamImpl<>(mockGraph, mockInputOp);
    when(mockContext.getMessageStream(eq(1))).thenReturn(mockStream);
    doAnswer(this.getRegisterMessageStreamAnswer()).when(mockContext).registerMessageStream(eq(2), any(MessageStream.class));
    RexToJavaCompiler mockCompiler = mock(RexToJavaCompiler.class);
    when(mockContext.getExpressionCompiler()).thenReturn(mockCompiler);
    Expression mockExpr = mock(Expression.class);
    when(mockCompiler.compile(any(), any())).thenReturn(mockExpr);

    // Apply translate() method to verify that we are getting the correct filter operator constructed
    FilterTranslator filterTranslator = new FilterTranslator();
    filterTranslator.translate(mockFilter, mockContext);
    // make sure that context has been registered with LogicFilter and output message streams
    verify(mockContext, times(1)).registerRelNode(2, mockFilter);
    verify(mockContext, times(1)).registerMessageStream(2, this.getRegisteredMessageStream(2));
    when(mockContext.getRelNode(2)).thenReturn(mockFilter);
    when(mockContext.getMessageStream(2)).thenReturn(this.getRegisteredMessageStream(2));
    StreamOperatorSpec filterSpec = (StreamOperatorSpec) Whitebox.getInternalState(this.getRegisteredMessageStream(2), "operatorSpec");
    assertNotNull(filterSpec);
    assertEquals(filterSpec.getOpCode(), OperatorSpec.OpCode.FILTER);

    // Verify that the bootstrap() method will establish the context for the filter function
    Config mockConfig = mock(Config.class);
    TaskContextImpl taskContext = new TaskContextImpl(new TaskName("Partition-1"), null, null,
        new HashSet<>(), null, null, null, null, null, null);
    taskContext.setUserContext(mockContext);
    filterSpec.getTransformFn().init(mockConfig, taskContext);
    FilterFunction filterFn = (FilterFunction) Whitebox.getInternalState(filterSpec, "filterFn");
    assertNotNull(filterFn);
    assertEquals(mockContext, Whitebox.getInternalState(filterFn, "context"));
    assertEquals(mockFilter, Whitebox.getInternalState(filterFn, "filter"));
    assertEquals(mockExpr, Whitebox.getInternalState(filterFn, "expr"));

    // Calling filterFn.apply() to verify the filter function is correctly applied to the input message
    SamzaSqlRelMessage mockInputMsg = new SamzaSqlRelMessage(new ArrayList<>(), new ArrayList<>());
    SamzaSqlExecutionContext executionContext = mock(SamzaSqlExecutionContext.class);
    DataContext dataContext = mock(DataContext.class);
    when(mockContext.getExecutionContext()).thenReturn(executionContext);
    when(mockContext.getDataContext()).thenReturn(dataContext);
    Object[] result = new Object[1];

    doAnswer( invocation -> {
      Object[] retValue = invocation.getArgumentAt(3, Object[].class);
      retValue[0] = new Boolean(true);
      return null;
    }).when(mockExpr).execute(eq(executionContext), eq(dataContext),
        eq(mockInputMsg.getSamzaSqlRelRecord().getFieldValues().toArray()), eq(result));
    assertTrue(filterFn.apply(mockInputMsg));

    doAnswer( invocation -> {
      Object[] retValue = invocation.getArgumentAt(3, Object[].class);
      retValue[0] = new Boolean(false);
      return null;
    }).when(mockExpr).execute(eq(executionContext), eq(dataContext),
        eq(mockInputMsg.getSamzaSqlRelRecord().getFieldValues().toArray()), eq(result));
    assertFalse(filterFn.apply(mockInputMsg));

  }

}
