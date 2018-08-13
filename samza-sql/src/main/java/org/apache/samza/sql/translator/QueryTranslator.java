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
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.task.TaskContext;
import org.apache.samza.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to populate the StreamGraph using the SQL queries.
 * This class contains the core of the SamzaSQL control code that converts the SQL statements to calcite relational graph.
 * It then walks the relational graph and then populates the Samza's {@link StreamGraph} accordingly.
 */
public class QueryTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(QueryTranslator.class);

  private final ScanTranslator scanTranslator;
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, SamzaRelConverter> converters;

  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private final String outputTopic;

    OutputMapFunction(String outputTopic) {
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
    final SqlIOResolver ioResolver = context.getExecutionContext().getSamzaSqlApplicationConfig().getIoResolver();

    node.accept(new RelShuttleImpl() {
      int windowId = 0;
      int joinId = 0;

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
        joinId++;
        new JoinTranslator(joinId, ioResolver).translate(join, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        windowId++;
        new LogicalAggregateTranslator(windowId).translate(aggregate, context);
        return node;
      }
    });

    String sink = queryInfo.getSink();
    SqlIOConfig sinkConfig = sqlConfig.getOutputSystemStreamConfigsBySource().get(sink);
    MessageStreamImpl<SamzaSqlRelMessage> stream = (MessageStreamImpl<SamzaSqlRelMessage>) context.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(sink));

    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
      String systemName = sinkConfig.getSystemName();
      GenericSystemDescriptor sd = context.getSystemDescriptors().computeIfAbsent(systemName, GenericSystemDescriptor::new);
      GenericOutputDescriptor<KV<Object, Object>> osd = sd.getOutputDescriptor(sinkConfig.getStreamName(), noOpKVSerde);
      outputStream.sendTo(streamGraph.getOutputStream(osd));
    } else {
      Table outputTable = streamGraph.getTable(tableDescriptor.get());
      if (outputTable == null) {
        String msg = "Failed to obtain table descriptor of " + sinkConfig.getSource();
        LOG.error(msg);
        throw new SamzaException(msg);
      }
      outputStream.sendTo(outputTable);
    }

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
