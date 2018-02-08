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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;


/**
 * Tests for {@link FilterTranslator}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(LogicalFilter.class)
public class TestFilterTranslator {

  @Test
  public void testTranslate() {
    LogicalFilter mockFilter = mock(LogicalFilter.class);
    TranslatorContext mockContext = mock(TranslatorContext.class);
    RelNode mockInput = mock(RelNode.class);
    when(mockFilter.getInput()).thenReturn(mockInput);
    when(mockInput.getId()).thenReturn(1);
    MessageStream<SamzaSqlRelMessage> mockStream = mock(MessageStream.class);
    when(mockContext.getMessageStream(eq(1))).thenReturn(mockStream);
    RexToJavaCompiler mockCompiler = mock(RexToJavaCompiler.class);
    when(mockContext.getExpressionCompiler()).thenReturn(mockCompiler);
    Expression mockExpr = mock(Expression.class);
    when(mockCompiler.compile(any(), any())).thenReturn(mockExpr);
    mockContext.registerMessageStream(eq(2), any());

    FilterTranslator filterTranslator = new FilterTranslator();
    filterTranslator.translate(mockFilter, mockContext);
  }

}
