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

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.task.TaskContext;


/**
 * This class is used to populate the StreamGraph using the SQL queries.
 * This class contains the core of the SamzaSQL control code that converts the SQL statements to calcite relational graph.
 * It then walks the relational graph and then populates the Samza's {@link StreamGraph} accordingly.
 */
public class QueryTranslator {

  private final ScanTranslator scanTranslator;
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, SamzaRelConverter> converters;

  private static class MapToOutput implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private final String outputTopic;

    MapToOutput(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    @Override
    public void init(Config config, TaskContext taskContext) {
      TranslatorContext context = (TranslatorContext) taskContext.getUserContext();
      this.samzaMsgConverter = context.getMsgConverter(outputTopic);
    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      return this.samzaMsgConverter.convertToSamzaMessage(message);
    }
  }

  public QueryTranslator(SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
    scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource());
    this.converters = sqlConfig.getSamzaRelConverters();
  }

  public void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamGraph streamGraph) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getInputSystemStreamConfigBySource(),
            sqlConfig.getUdfMetadata());
    final SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(this.sqlConfig);
    final RelRoot relRoot = planner.plan(queryInfo.getSelectQuery());
    final TranslatorContext context = new TranslatorContext(streamGraph, relRoot, executionContext, this.converters);
    final RelNode node = relRoot.project();
    final int[] joinId = new int[1];

    node.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        scanTranslator.translate(scan, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode node = visitChild(filter, 0, filter.getInput());
        new FilterTranslator().translate(filter, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator().translate(project, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode node = super.visit(join);
        joinId[0]++;
        SourceResolver sourceResolver = context.getExecutionContext().getSamzaSqlApplicationConfig().getSourceResolver();
        new JoinTranslator(joinId[0], sourceResolver).translate(join, context);
        return node;
      }
    });

    SqlSystemSourceConfig outputSystemConfig =
        sqlConfig.getOutputSystemStreamConfigsBySource().get(queryInfo.getOutputSource());
    final String outputTopic = queryInfo.getOutputSource();
    MessageStreamImpl<SamzaSqlRelMessage> stream =
        (MessageStreamImpl<SamzaSqlRelMessage>) context.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new MapToOutput(outputTopic));

    outputStream.sendTo(streamGraph.getOutputStream(outputSystemConfig.getStreamName()));

    streamGraph.withContextManager(new ContextManager() {
      @Override
      public void init(Config config, TaskContext taskContext) {
        taskContext.setUserContext(context.clone());
      }

      @Override
      public void close() {

      }

    });

  }
}
