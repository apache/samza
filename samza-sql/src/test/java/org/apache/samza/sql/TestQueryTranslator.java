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

package org.apache.samza.sql;

import java.util.HashSet;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.apache.samza.sql.translator.QueryTranslator;
import org.apache.samza.sql.translator.TranslatorContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.mockito.Mockito.*;


public class TestQueryTranslator {

  // Helper functions to validate the cloned copies of TranslatorContext and SamzaSqlExecutionContext
  private void validateClonedTranslatorContext(TranslatorContext originContext, TranslatorContext clonedContext) {
    Assert.assertNotEquals(originContext, clonedContext);
    Assert.assertTrue(originContext.getExpressionCompiler() == clonedContext.getExpressionCompiler());
    Assert.assertTrue(originContext.getStreamGraph() == clonedContext.getStreamGraph());
    Assert.assertTrue(originContext.getExpressionCompiler() == clonedContext.getExpressionCompiler());
    Assert.assertTrue(Whitebox.getInternalState(originContext, "relSamzaConverters") == Whitebox.getInternalState(clonedContext, "relSamzaConverters"));
    Assert.assertTrue(Whitebox.getInternalState(originContext, "messsageStreams") == Whitebox.getInternalState(clonedContext, "messsageStreams"));
    Assert.assertTrue(Whitebox.getInternalState(originContext, "relNodes") == Whitebox.getInternalState(clonedContext, "relNodes"));
    Assert.assertNotEquals(originContext.getDataContext(), clonedContext.getDataContext());
    validateClonedExecutionContext(originContext.getExecutionContext(), clonedContext.getExecutionContext());
  }

  private void validateClonedExecutionContext(SamzaSqlExecutionContext originContext,
      SamzaSqlExecutionContext clonedContext) {
    Assert.assertNotEquals(originContext, clonedContext);
    Assert.assertTrue(Whitebox.getInternalState(originContext, "sqlConfig") == Whitebox.getInternalState(clonedContext, "sqlConfig"));
    Assert.assertTrue(Whitebox.getInternalState(originContext, "udfMetadata") == Whitebox.getInternalState(clonedContext, "udfMetadata"));
    Assert.assertTrue(Whitebox.getInternalState(originContext, "udfInstances") != Whitebox.getInternalState(clonedContext, "udfInstances"));
  }

  @Test
  public void testTranslate() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic select MyTest(id) from testavro.level1.level2.SIMPLE1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config));
    QueryTranslator translator = new QueryTranslator(samzaSqlApplicationConfig);
    SamzaSqlQueryParser.QueryInfo queryInfo = samzaSqlApplicationConfig.getQueryInfo().get(0);
    StreamGraphImpl streamGraph = new StreamGraphImpl(new LocalApplicationRunner(samzaConfig), samzaConfig);
    translator.translate(queryInfo, streamGraph);
    Assert.assertEquals(1, streamGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("outputTopic", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getPhysicalName());
    Assert.assertEquals(1, streamGraph.getInputOperators().size());
    Assert.assertEquals("testavro",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("SIMPLE1",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getPhysicalName());

    // make sure that each task context would have a separate instance of cloned TranslatorContext
    TaskContextImpl testContext = new TaskContextImpl(new TaskName("Partition 1"), null, null,
        new HashSet<>(), null, null, null, null, null, null);
    // call ContextManager.init() to instantiate the per-task TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertNotNull(testContext.getUserContext());
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    TranslatorContext contextPerTaskOne = (TranslatorContext) testContext.getUserContext();
    // call ContextManager.init() second time to instantiate another clone of TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    // validate the two copies of TranslatorContext are clones of each other
    validateClonedTranslatorContext(contextPerTaskOne, (TranslatorContext) testContext.getUserContext());
  }

  @Test
  public void testTranslateComplex() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic select Flatten(array_values) from testavro.COMPLEX1");
//    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
//        "Insert into testavro.foo2 select string_value, SUM(id) from testavro.COMPLEX1 "
//            + "GROUP BY TumbleWindow(CURRENT_TIME, INTERVAL '1' HOUR), string_value");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config));
    QueryTranslator translator = new QueryTranslator(samzaSqlApplicationConfig);
    SamzaSqlQueryParser.QueryInfo queryInfo = samzaSqlApplicationConfig.getQueryInfo().get(0);
    StreamGraphImpl streamGraph = new StreamGraphImpl(new LocalApplicationRunner(samzaConfig), samzaConfig);
    translator.translate(queryInfo, streamGraph);
    Assert.assertEquals(1, streamGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("outputTopic", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getPhysicalName());
    Assert.assertEquals(1, streamGraph.getInputOperators().size());
    Assert.assertEquals("testavro",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("COMPLEX1",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getPhysicalName());

    // make sure that each task context would have a separate instance of cloned TranslatorContext
    TaskContextImpl testContext = new TaskContextImpl(new TaskName("Partition 1"), null, null,
        new HashSet<>(), null, null, null, null, null, null);
    // call ContextManager.init() to instantiate the per-task TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertNotNull(testContext.getUserContext());
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    TranslatorContext contextPerTaskOne = (TranslatorContext) testContext.getUserContext();
    // call ContextManager.init() second time to instantiate another clone of TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    // validate the two copies of TranslatorContext are clones of each other
    validateClonedTranslatorContext(contextPerTaskOne, (TranslatorContext) testContext.getUserContext());
  }

  @Test
  public void testTranslateSubQuery() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic select Flatten(a), id from (select id, array_values a, string_value s from testavro.COMPLEX1)");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config));
    QueryTranslator translator = new QueryTranslator(samzaSqlApplicationConfig);
    SamzaSqlQueryParser.QueryInfo queryInfo = samzaSqlApplicationConfig.getQueryInfo().get(0);
    StreamGraphImpl streamGraph = new StreamGraphImpl(new LocalApplicationRunner(samzaConfig), samzaConfig);
    translator.translate(queryInfo, streamGraph);
    Assert.assertEquals(1, streamGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("outputTopic", streamGraph.getOutputStreams().keySet().stream().findFirst().get().getPhysicalName());
    Assert.assertEquals(1, streamGraph.getInputOperators().size());
    Assert.assertEquals("testavro",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getSystemName());
    Assert.assertEquals("COMPLEX1",
        streamGraph.getInputOperators().keySet().stream().findFirst().get().getPhysicalName());

    // make sure that each task context would have a separate instance of cloned TranslatorContext
    TaskContextImpl testContext = new TaskContextImpl(new TaskName("Partition 1"), null, null,
        new HashSet<>(), null, null, null, null, null, null);
    // call ContextManager.init() to instantiate the per-task TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertNotNull(testContext.getUserContext());
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    TranslatorContext contextPerTaskOne = (TranslatorContext) testContext.getUserContext();
    // call ContextManager.init() second time to instantiate another clone of TranslatorContext
    streamGraph.getContextManager().init(samzaConfig, testContext);
    Assert.assertTrue(testContext.getUserContext() instanceof TranslatorContext);
    // validate the two copies of TranslatorContext are clones of each other
    validateClonedTranslatorContext(contextPerTaskOne, (TranslatorContext) testContext.getUserContext());
  }
}
