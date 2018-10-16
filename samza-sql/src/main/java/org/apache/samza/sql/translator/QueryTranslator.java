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
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;


/**
 * This class is used to populate the {@link StreamApplicationDescriptor} using the SQL queries.
 * This class contains the core of the SamzaSQL control code that converts the SQL statements to calcite relational graph.
 * It then walks the relational graph and then populates the Samza's {@link StreamApplicationDescriptor} accordingly.
 */
public class QueryTranslator {
  private final ScanTranslator scanTranslator;
  private final ModifyTranslator modifyTranslator;
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, SamzaRelConverter> converters;

  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private final String outputTopic;

    OutputMapFunction(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContext();
      this.samzaMsgConverter = translatorContext.getMsgConverter(outputTopic);
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
    modifyTranslator =
        new ModifyTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getOutputSystemStreamConfigsBySource());
    this.converters = sqlConfig.getSamzaRelConverters();
  }

  public void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamApplicationDescriptor appDesc) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(queryInfo.getSql());
    translate(relRoot, appDesc);
  }

  public void translate(RelRoot relRoot, StreamApplicationDescriptor appDesc) {
    final SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(this.sqlConfig);
    final TranslatorContext translatorContext = new TranslatorContext(appDesc, relRoot, executionContext, this.converters);
    final SqlIOResolver ioResolver = translatorContext.getExecutionContext().getSamzaSqlApplicationConfig().getIoResolver();
    final RelNode node = relRoot.project();

    node.accept(new RelShuttleImpl() {
      int windowId = 0;
      int joinId = 0;

      @Override
      public RelNode visit(RelNode relNode) {
        if (relNode instanceof TableModify) {
          return visit((TableModify) relNode);
        }
        return super.visit(relNode);
      }

      private RelNode visit(TableModify modify) {
        if (!modify.isInsert()) {
          throw new SamzaException("Not a supported operation: " + modify.toString());
        }
        RelNode node = super.visit(modify);
        modifyTranslator.translate(modify, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        scanTranslator.translate(scan, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode node = visitChild(filter, 0, filter.getInput());
        new FilterTranslator().translate(filter, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator().translate(project, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode node = super.visit(join);
        joinId++;
        new JoinTranslator(joinId, ioResolver).translate(join, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        windowId++;
        new LogicalAggregateTranslator(windowId).translate(aggregate, translatorContext);
        return node;
      }
    });

    // the snippet below will be performed only when sql is a query statement
    sqlConfig.getOutputSystemStreamConfigsBySource().keySet().forEach(
        key -> {
          if (key.split("\\.")[0].equals(SamzaSqlApplicationConfig.SAMZA_SYSTEM_LOG)) {
            sendToOutputStream(appDesc, translatorContext, node, key);
          }
        }
    );

    /*
     * TODO When serialization of ApplicationDescriptor is actually needed, then something will need to be updated here,
     * since translatorContext is not Serializable. Currently, a new ApplicationDescriptor instance is created in each
     * container, so it does not need to be serialized. Therefore, the translatorContext is recreated in each container
     * and does not need to be serialized.
     */
    appDesc.withApplicationTaskContextFactory((jobContext,
        containerContext,
        taskContext,
        applicationContainerContext) ->
        new SamzaSqlApplicationContext(translatorContext.clone()));
  }

  private void sendToOutputStream(StreamApplicationDescriptor appDesc, TranslatorContext context, RelNode node, String sink) {
    SqlIOConfig sinkConfig = sqlConfig.getOutputSystemStreamConfigsBySource().get(sink);
    MessageStream<SamzaSqlRelMessage> stream = context.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(sink));
    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
      String systemName = sinkConfig.getSystemName();
      DelegatingSystemDescriptor
          sd = context.getSystemDescriptors().computeIfAbsent(systemName, DelegatingSystemDescriptor::new);
      GenericOutputDescriptor<KV<Object, Object>> osd = sd.getOutputDescriptor(sinkConfig.getStreamName(), noOpKVSerde);
      outputStream.sendTo(appDesc.getOutputStream(osd));
    } else {
      Table outputTable = appDesc.getTable(tableDescriptor.get());
      if (outputTable == null) {
        String msg = "Failed to obtain table descriptor of " + sinkConfig.getSource();
        throw new SamzaException(msg);
      }
      outputStream.sendTo(outputTable);
    }
  }
}