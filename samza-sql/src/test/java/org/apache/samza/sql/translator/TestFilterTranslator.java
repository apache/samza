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
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.sql.util.TestMetricsRegistryImpl;
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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
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
  final private String LOGICAL_OP_ID = "sql0_filter_0";


  @Test
  public void testTranslate() throws IOException, ClassNotFoundException {
    // setup mock values to the constructor of FilterTranslator
    LogicalFilter mockFilter = PowerMockito.mock(LogicalFilter.class);
    Context mockContext = mock(Context.class);
    ContainerContext mockContainerContext = mock(ContainerContext.class);
    TranslatorContext mockTranslatorContext = mock(TranslatorContext.class);
    TestMetricsRegistryImpl metricsRegistry = new TestMetricsRegistryImpl();
    RelNode mockInput = mock(RelNode.class);
    when(mockFilter.getInput()).thenReturn(mockInput);
    when(mockInput.getId()).thenReturn(1);
    when(mockFilter.getId()).thenReturn(2);
    StreamApplicationDescriptorImpl mockGraph = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpec<Object, SamzaSqlRelMessage> mockInputOp = mock(OperatorSpec.class);
    MessageStream<SamzaSqlRelMessage> mockStream = new MessageStreamImpl<>(mockGraph, mockInputOp);
    when(mockTranslatorContext.getMessageStream(eq(1))).thenReturn(mockStream);
    doAnswer(this.getRegisterMessageStreamAnswer()).when(mockTranslatorContext).registerMessageStream(eq(2), any(MessageStream.class));
    RexToJavaCompiler mockCompiler = mock(RexToJavaCompiler.class);
    when(mockTranslatorContext.getExpressionCompiler()).thenReturn(mockCompiler);
    Expression mockExpr = mock(Expression.class);
    when(mockCompiler.compile(any(), any())).thenReturn(mockExpr);
    when(mockContext.getContainerContext()).thenReturn(mockContainerContext);
    when(mockContainerContext.getContainerMetricsRegistry()).thenReturn(metricsRegistry);

    // Apply translate() method to verify that we are getting the correct filter operator constructed
    FilterTranslator filterTranslator = new FilterTranslator(1);
    filterTranslator.translate(mockFilter, LOGICAL_OP_ID, mockTranslatorContext);
    // make sure that context has been registered with LogicFilter and output message streams
    verify(mockTranslatorContext, times(1)).registerRelNode(2, mockFilter);
    verify(mockTranslatorContext, times(1)).registerMessageStream(2, this.getRegisteredMessageStream(2));
    when(mockTranslatorContext.getRelNode(2)).thenReturn(mockFilter);
    when(mockTranslatorContext.getMessageStream(2)).thenReturn(this.getRegisteredMessageStream(2));
    StreamOperatorSpec filterSpec = (StreamOperatorSpec) Whitebox.getInternalState(this.getRegisteredMessageStream(2), "operatorSpec");
    assertNotNull(filterSpec);
    assertEquals(filterSpec.getOpCode(), OperatorSpec.OpCode.FILTER);

    // Verify that the describe() method will establish the context for the filter function
    Map<Integer, TranslatorContext> mockContexts= new HashMap<>();
    mockContexts.put(1, mockTranslatorContext);
    when(mockContext.getApplicationTaskContext()).thenReturn(new SamzaSqlApplicationContext(mockContexts));
    filterSpec.getTransformFn().init(mockContext);
    FilterFunction filterFn = (FilterFunction) Whitebox.getInternalState(filterSpec, "filterFn");
    assertNotNull(filterFn);
    assertEquals(mockTranslatorContext, Whitebox.getInternalState(filterFn, "translatorContext"));
    assertEquals(mockFilter, Whitebox.getInternalState(filterFn, "filter"));
    assertEquals(mockExpr, Whitebox.getInternalState(filterFn, "expr"));
    // Verify MetricsRegistry works with Project
    assertEquals(1, metricsRegistry.getGauges().size());
    assertTrue(metricsRegistry.getGauges().get(LOGICAL_OP_ID).size() > 0);
    assertEquals(1, metricsRegistry.getCounters().size());
    assertEquals(3, metricsRegistry.getCounters().get(LOGICAL_OP_ID).size());
    assertEquals(0, metricsRegistry.getCounters().get(LOGICAL_OP_ID).get(0).getCount());
    assertEquals(0, metricsRegistry.getCounters().get(LOGICAL_OP_ID).get(1).getCount());

    // Calling filterFn.apply() to verify the filter function is correctly applied to the input message
    SamzaSqlRelMessage mockInputMsg = new SamzaSqlRelMessage(new ArrayList<>(), new ArrayList<>(),
        new SamzaSqlRelMsgMetadata(0, 0, 0));
    SamzaSqlExecutionContext executionContext = mock(SamzaSqlExecutionContext.class);
    DataContext dataContext = mock(DataContext.class);
    when(mockTranslatorContext.getExecutionContext()).thenReturn(executionContext);
    when(mockTranslatorContext.getDataContext()).thenReturn(dataContext);
    Object[] result = new Object[1];

    doAnswer( invocation -> {
      Object[] retValue = invocation.getArgumentAt(4, Object[].class);
      retValue[0] = new Boolean(true);
      return null;
    }).when(mockExpr).execute(eq(executionContext), eq(mockContext), eq(dataContext),
        eq(mockInputMsg.getSamzaSqlRelRecord().getFieldValues().toArray()), eq(result));
    assertTrue(filterFn.apply(mockInputMsg));

    doAnswer( invocation -> {
      Object[] retValue = invocation.getArgumentAt(4, Object[].class);
      retValue[0] = new Boolean(false);
      return null;
    }).when(mockExpr).execute(eq(executionContext), eq(mockContext), eq(dataContext),
        eq(mockInputMsg.getSamzaSqlRelRecord().getFieldValues().toArray()), eq(result));
    assertFalse(filterFn.apply(mockInputMsg));

    // Verify filterFn.apply() updates the MetricsRegistry metrics
    assertEquals(2, metricsRegistry.getCounters().get(LOGICAL_OP_ID).get(0).getCount());
    assertEquals(1, metricsRegistry.getCounters().get(LOGICAL_OP_ID).get(1).getCount());

  }

}
