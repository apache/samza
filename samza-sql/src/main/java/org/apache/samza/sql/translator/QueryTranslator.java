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
import org.apache.commons.lang.Validate;
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
  void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamApplicationDescriptor appDesc, int queryId) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getInputSystemStreamConfigBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(queryInfo.getSelectQuery());
    SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(sqlConfig);
    TranslatorContext translatorContext = new TranslatorContext(appDesc, relRoot, executionContext);
    translate(relRoot, sqlConfig.getOutputSystemStreams().get(queryId), translatorContext, queryId);
    Map<Integer, TranslatorContext> translatorContexts = new HashMap<>();
    translatorContexts.put(queryId, translatorContext.clone());
    appDesc.withApplicationTaskContextFactory((jobContext,
        containerContext,
        taskContext,
        applicationContainerContext) ->
        new SamzaSqlApplicationContext(translatorContexts));
  }

  /**
   * Translate Calcite plan to Samza stream operators.
   * @param relRoot Calcite plan in the form of {@link RelRoot}. RelRoot should not include the sink ({@link TableModify})
   * @param outputSystemStream Sink associated with the Calcite plan.
   * @param translatorContext Context maintained across translations.
   * @param queryId query index of the sql statement corresponding to the Calcite plan in multi SQL statement scenario
   *                starting with index 0.
   */
  public void translate(RelRoot relRoot, String outputSystemStream, TranslatorContext translatorContext, int queryId) {
    final RelNode node = relRoot.project();

    ScanTranslator scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource(), queryId);

    node.accept(new RelShuttleImpl() {
      int windowId = 0;
      int joinId = 0;

      @Override
      public RelNode visit(RelNode relNode) {
        // There should never be a TableModify in the calcite plan.
        Validate.isTrue(!(relNode instanceof TableModify));
        return super.visit(relNode);
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

    sendToOutputStream(outputSystemStream, streamAppDescriptor, translatorContext, node, queryId);
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
      OutputStream stm = outputMsgStreams.computeIfAbsent(sinkConfig.getSource(), v -> appDesc.getOutputStream(osd));
      outputStream.sendTo(stm);
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
