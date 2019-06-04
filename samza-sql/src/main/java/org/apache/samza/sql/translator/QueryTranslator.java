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
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.context.TaskContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.SamzaSqlInputMessage;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.sql.util.SamzaSqlQueryParser;
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
  private final Map<String, MessageStream<SamzaSqlInputMessage>> inputMsgStreams;
  private final Map<String, OutputStream> outputMsgStreams;
  private static final Logger LOG = LoggerFactory.getLogger(QueryTranslator.class);
  static int opId = 0;

  /**
   * map function used by SendToOutputStram to convert SamzaRelMessage to KV
   * it also maintains SendTo and most Query metrics
   */
  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private transient MetricsRegistry metricsRegistry;
    /**
     * TODO: [SAMZA-2031]: the time-based metrics here for insert and query are
     * currently not accurate because they don't include the time of sendTo() call
     * It is not feasible to include it because sendTo operator does not return
     * a stream to process its messages to update hte metrics.
     */
    /* insert (SendToOutputStream) metrics */
    private transient SamzaHistogram insertProcessingTime;
    /* query metrics */
    private transient SamzaHistogram totalLatency; // (if event time exists) = output time - event time (msec)
    private transient SamzaHistogram queryLatency; // = output time - scan time (msec)
    private transient SamzaHistogram queueingLatency; // = scan time - arrival time (msec)
    private transient Counter queryOutputEvents;

    private final String outputTopic;
    private final int queryId;
    private String queryLogicalId;
    private String insertLogicalId;

    OutputMapFunction(String queryLogicalId, String insertLogicalId, String outputTopic, int queryId) {
      this.outputTopic = outputTopic;
      this.queryId = queryId;
      this.queryLogicalId = queryLogicalId;
      this.insertLogicalId = insertLogicalId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.samzaMsgConverter = translatorContext.getMsgConverter(outputTopic);
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      /* insert (SendToOutputStream) metrics */
      insertProcessingTime =
          new SamzaHistogram(metricsRegistry, insertLogicalId, TranslatorConstants.TOTAL_LATENCY_NAME);

      /* query metrics */
      totalLatency = new SamzaHistogram(metricsRegistry, queryLogicalId, TranslatorConstants.TOTAL_LATENCY_NAME);

      queryLatency = new SamzaHistogram(metricsRegistry, queryLogicalId, TranslatorConstants.QUERY_LATENCY_NAME);
      queueingLatency = new SamzaHistogram(metricsRegistry, queryLogicalId, TranslatorConstants.QUEUEING_LATENCY_NAME);
      
      queryOutputEvents = metricsRegistry.newCounter(queryLogicalId, TranslatorConstants.OUTPUT_EVENTS_NAME);
      queryOutputEvents.clear();
    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      Instant beginProcessing = Instant.now();
      KV<Object, Object> retKV = this.samzaMsgConverter.convertToSamzaMessage(message);
      if (message.getSamzaSqlRelRecord().containsField(SamzaSqlRelMessage.OP_NAME)
          && ((String) message.getSamzaSqlRelRecord().getField(SamzaSqlRelMessage.OP_NAME).get()).equalsIgnoreCase(
          SamzaSqlRelMessage.DELETE_OP)) {
        // If it is a delete op. Set the payload to null so that the record gets deleted.
        retKV = new KV<>(retKV.key, null);
      }
      updateMetrics(beginProcessing, Instant.now(), message.getSamzaSqlRelMsgMetadata());
      return retKV;
    }

    /**
     * Updates the Diagnostics Metrics (processing time and number of events)
     * @param beginProcessing when sendOutput Started processing this message
     * @param endProcessing when sendOutput finished processing this message
     * @param metadata the event's message metadata
     */
    private void updateMetrics(Instant beginProcessing, Instant endProcessing, SamzaSqlRelMsgMetadata metadata) {
      /* insert (SendToOutputStream) metrics */
      insertProcessingTime.update(Duration.between(beginProcessing, endProcessing).toMillis());
      /* query metrics */
      Instant outputTime = Instant.now();
      queryOutputEvents.inc();
      /* TODO: remove scanTime validation once code to assign it is stable */
      Validate.isTrue(metadata.hasScanTime());
      Instant scanTime = Instant.parse(metadata.getscanTime());
      queryLatency.update(Duration.between(scanTime, outputTime).toMillis());
      /** TODO: change if hasArrivalTime to validation once arrivalTime is assigned,
       and later remove the check once code is stable */
      if (metadata.hasArrivalTime()) {
        Instant arrivalTime = Instant.parse(metadata.getarrivalTime());
        queueingLatency.update(Duration.between(arrivalTime, scanTime).toMillis());
      }
      /* since availability of eventTime depends on source, we need the following check */
      if (metadata.hasEventTime()) {
        Instant eventTime = Instant.parse(metadata.getEventTime());
        totalLatency.update(Duration.between(eventTime, outputTime).toMillis());
      }
    }
  } // OutputMapFunction

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
    appDesc.withApplicationTaskContextFactory(new ApplicationTaskContextFactory<SamzaSqlApplicationContext>() {
      @Override
      public SamzaSqlApplicationContext create(ExternalContext externalContext, JobContext jobContext,
          ContainerContext containerContext, TaskContext taskContext,
          ApplicationContainerContext applicationContainerContext) {
        return new SamzaSqlApplicationContext(translatorContexts);
      }
    });
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

    /* update input metrics */
    String queryLogicalId = String.format(TranslatorConstants.LOGSQLID_TEMPLATE, queryId);

    opId = 0;

    node.accept(new RelShuttleImpl() {

      @Override
      public RelNode visit(RelNode relNode) {
        // There should never be a TableModify in the calcite plan.
        Validate.isTrue(!(relNode instanceof TableModify));
        return super.visit(relNode);
      }

      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "scan", opId++);
        scanTranslator.translate(scan, queryLogicalId, logicalOpId, translatorContext, systemDescriptors,
            inputMsgStreams);
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
        new JoinTranslator(logicalOpId, sqlConfig.getMetadataTopicPrefix(), queryId).translate(join, translatorContext);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "window", opId++);
        new LogicalAggregateTranslator(logicalOpId, sqlConfig.getMetadataTopicPrefix()).translate(aggregate,
            translatorContext);
        return node;
      }
    });

    String logicalOpId = String.format(TranslatorConstants.LOGOPID_TEMPLATE, queryId, "insert", opId);
    sendToOutputStream(queryLogicalId, logicalOpId, outputSystemStream, streamAppDescriptor, translatorContext, node,
        queryId);
  }

  private void sendToOutputStream(String queryLogicalId, String logicalOpId, String sinkStream,
      StreamApplicationDescriptor appDesc, TranslatorContext translatorContext, RelNode node, int queryId) {
    SqlIOConfig sinkConfig = sqlConfig.getOutputSystemStreamConfigsBySource().get(sinkStream);
    MessageStream<SamzaSqlRelMessage> stream = translatorContext.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream =
        stream.map(new OutputMapFunction(queryLogicalId, logicalOpId, sinkStream, queryId));
    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
      String systemName = sinkConfig.getSystemName();
      DelegatingSystemDescriptor sd = systemDescriptors.computeIfAbsent(systemName, DelegatingSystemDescriptor::new);
      GenericOutputDescriptor<KV<Object, Object>> osd = sd.getOutputDescriptor(sinkConfig.getStreamId(), noOpKVSerde);
      OutputStream stm = outputMsgStreams.computeIfAbsent(sinkConfig.getSource(), v -> appDesc.getOutputStream(osd));
      outputStream.sendTo(stm);

      // Process system events only if the output is a stream.
      if (sqlConfig.isProcessSystemEvents()) {
        for (MessageStream<SamzaSqlInputMessage> inputStream : inputMsgStreams.values()) {
          MessageStream<KV<Object, Object>> systemEventStream =
              inputStream.filter(message -> message.getMetadata().isSystemMessage())
                  .map(SamzaSqlInputMessage::getKeyAndMessageKV);

          systemEventStream.sendTo(stm);
        }
      }
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
