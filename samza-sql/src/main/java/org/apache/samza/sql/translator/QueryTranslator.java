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
import java.util.HashMap;
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
import org.apache.samza.operators.OutputStream;
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
  private final SamzaSqlApplicationConfig sqlConfig;
  private final StreamApplicationDescriptor streamAppDescriptor;
  private final Map<String, DelegatingSystemDescriptor> systemDescriptors;
  private final Map<String, MessageStream<KV<Object, Object>>> inputMsgStreams;
  private final Map<String, OutputStream> outputMsgStreams;

  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private final String outputTopic;
    private final int queryId;
    static OutputStream logOutputStream;

    OutputMapFunction(String outputTopic, int queryId) {
      this.outputTopic = outputTopic;
      this.queryId = queryId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.samzaMsgConverter = translatorContext.getMsgConverter(outputTopic);
    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      return this.samzaMsgConverter.convertToSamzaMessage(message);
    }
  }

  public QueryTranslator(StreamApplicationDescriptor appDesc, SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
    this.streamAppDescriptor = appDesc;
    this.systemDescriptors = new HashMap<>();
    this.outputMsgStreams = new HashMap<>();
    this.inputMsgStreams = new HashMap<>();
  }

  /**
   * For unit testing only
   */
  @VisibleForTesting
  public void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamApplicationDescriptor appDesc, int queryId) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(queryInfo.getSql());
    SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(sqlConfig);
    TranslatorContext translatorContext = new TranslatorContext(appDesc, relRoot, executionContext);
    translate(relRoot, translatorContext, queryId);
    Map<Integer, TranslatorContext> translatorContexts = new HashMap<>();
    translatorContexts.put(queryId, translatorContext.clone());
    appDesc.withApplicationTaskContextFactory((jobContext,
        containerContext,
        taskContext,
        applicationContainerContext) ->
        new SamzaSqlApplicationContext(translatorContexts));
  }

  public void translate(RelRoot relRoot, TranslatorContext translatorContext, int queryId) {
    final RelNode node = relRoot.project();
    ScanTranslator scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource(), queryId);
    ModifyTranslator modifyTranslator =
        new ModifyTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getOutputSystemStreamConfigsBySource(), queryId);

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
        modifyTranslator.translate(modify, translatorContext, systemDescriptors, outputMsgStreams);
        return node;
      }

      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        scanTranslator.translate(scan, translatorContext, systemDescriptors, inputMsgStreams);
        return node;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode node = visitChild(filter, 0, filter.getInput());
        new FilterTranslator(queryId).translate(filter, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator(queryId).translate(project, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode node = super.visit(join);
        joinId++;
        new JoinTranslator(joinId, sqlConfig.getMetadataTopicPrefix(), queryId)
            .translate(join, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        windowId++;
        new LogicalAggregateTranslator(windowId, sqlConfig.getMetadataTopicPrefix())
            .translate(aggregate, translatorContext);
        return node;
      }
    });

    // the snippet below will be performed only when sql is a query statement
    sqlConfig.getOutputSystemStreamConfigsBySource().keySet().forEach(
        key -> {
          if (key.split("\\.")[0].equals(SamzaSqlApplicationConfig.SAMZA_SYSTEM_LOG)) {
            sendToOutputStream(key, streamAppDescriptor, translatorContext, node, queryId);
          }
        }
    );
  }

  private void sendToOutputStream(String sinkStream, StreamApplicationDescriptor appDesc, TranslatorContext context, RelNode node, int queryId) {
    SqlIOConfig sinkConfig = sqlConfig.getOutputSystemStreamConfigsBySource().get(sinkStream);
    MessageStream<SamzaSqlRelMessage> stream = context.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(sinkStream, queryId));
    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
      String systemName = sinkConfig.getSystemName();
      DelegatingSystemDescriptor
          sd = systemDescriptors.computeIfAbsent(systemName, DelegatingSystemDescriptor::new);
      GenericOutputDescriptor<KV<Object, Object>> osd = sd.getOutputDescriptor(sinkConfig.getStreamId(), noOpKVSerde);
      if (OutputMapFunction.logOutputStream == null) {
        OutputMapFunction.logOutputStream = appDesc.getOutputStream(osd);
      }
      outputStream.sendTo(OutputMapFunction.logOutputStream);
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
