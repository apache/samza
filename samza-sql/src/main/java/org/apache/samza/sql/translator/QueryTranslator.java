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
import java.time.Duration;
import java.time.Instant;
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
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOG = LoggerFactory.getLogger(QueryTranslator.class);


  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private transient MetricsRegistry metricsRegistry;
    private transient SamzaHistogram latency; // milli-seconds
    private transient Counter outputEvents;
    private final String outputTopic;
    private final int queryId;
    static OutputStream logOutputStream;
    private String queryLogicalId;


    OutputMapFunction(String outputTopic, int queryId) {
      this.outputTopic = outputTopic;
      this.queryId = queryId;
      queryLogicalId = String.format(TranslatorConstants.LOGSQLID_TEMPLATE, queryId);
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.samzaMsgConverter = translatorContext.getMsgConverter(outputTopic);
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      latency = new SamzaHistogram(metricsRegistry, queryLogicalId, TranslatorConstants.LATENCY_NAME);
      outputEvents = metricsRegistry.newCounter(queryLogicalId, TranslatorConstants.OUTPUT_EVENTS_NAME);
      outputEvents.clear();
    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      if (!message.hasMeatadata()) {
        LOG.warn("Output message with no Metadata: " + message);
        message.setSamzaSqlRelMsgMetadata(new SamzaSqlRelMsgMetadata(Instant.now().toString()));
      }
      KV<Object, Object> retKV = this.samzaMsgConverter.convertToSamzaMessage(message);
      updateMetrics(message.getSamzaSqlRelMsgMetadata().getEventTime(), Instant.now());
      return  retKV;
    }

    /**
     * Updates the Diagnostics Metrics (processing time and number of events)
     * @param outputTime output message output time (=end of processing in this operator)
     */
    private void updateMetrics(String eventTime, Instant outputTime) {
      outputEvents.inc();
      latency.update(Duration.between(Instant.parse(eventTime), outputTime).toMillis());
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
      int opId = 0;

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
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "modify", opId++);
        modifyTranslator.translate(logicalOpId, modify, translatorContext, systemDescriptors, outputMsgStreams);
        return node;
      }

      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "scan", opId++);
        scanTranslator.translate(scan, logicalOpId, translatorContext, systemDescriptors, inputMsgStreams);
        return node;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode node = visitChild(filter, 0, filter.getInput());
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "filter", opId++);
        new FilterTranslator(queryId).translate(filter, logicalOpId, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "project", opId++);
        new ProjectTranslator(queryId).translate(project, logicalOpId, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode node = super.visit(join);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "join", opId++);
        new JoinTranslator(logicalOpId, sqlConfig.getMetadataTopicPrefix(), queryId)
            .translate(join, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "window", opId++);
        new LogicalAggregateTranslator(logicalOpId, sqlConfig.getMetadataTopicPrefix())
            .translate(aggregate, translatorContext);
        return node;
      }
    });

    // the snippet below will be performed only when sql is a query statement
    sqlConfig.getOutputSystemStreamConfigsBySource().keySet().forEach(
        key -> {
          if (key.split("\\.")[0].equals(SamzaSqlApplicationConfig.SAMZA_SYSTEM_LOG)) {
            sendToOutputStream(streamAppDescriptor, translatorContext, node, queryId);
          }
        }
    );
  }

  private void sendToOutputStream(StreamApplicationDescriptor appDesc, TranslatorContext translatorContext, RelNode node, int queryId) {
    SqlIOConfig sinkConfig = sqlConfig.getOutputSystemStreamConfigsBySource().get(SamzaSqlApplicationConfig.SAMZA_SYSTEM_LOG);
    MessageStream<SamzaSqlRelMessage> stream = translatorContext.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(SamzaSqlApplicationConfig.SAMZA_SYSTEM_LOG, queryId));
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
