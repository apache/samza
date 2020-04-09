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

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.SamzaSqlInputMessage;
import org.apache.samza.sql.SamzaSqlInputTransformer;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.InputTransformer;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;


/**
 * Translator to translate the TableScans in relational graph to the corresponding input streams in the StreamGraph
 * implementation
 */
class ScanTranslator {

  private final Map<String, SamzaRelConverter> relMsgConverters;
  private final Map<String, SqlIOConfig> systemStreamConfig;
  private final int queryId;

  // FilterFunction to filter out any messages that are system specific.
  private static class FilterSystemMessageFunction implements FilterFunction<SamzaSqlInputMessage> {
    private transient SamzaRelConverter relConverter;
    private final String source;
    private final int queryId;

    FilterSystemMessageFunction(String source, int queryId) {
      this.source = source;
      this.queryId = queryId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      relConverter = translatorContext.getMsgConverter(source);
    }

    @Override
    public boolean apply(SamzaSqlInputMessage samzaSqlInputMessage) {
      return !samzaSqlInputMessage.getMetadata().isSystemMessage();
    }
  }

  ScanTranslator(Map<String, SamzaRelConverter> converters, Map<String, SqlIOConfig> ssc, int queryId) {
    relMsgConverters = converters;
    this.systemStreamConfig = ssc;
    this.queryId = queryId;
  }

  /**
   * ScanMapFUnction implements MapFunction to process input SamzaSqlRelMessages into output
   * SamzaSqlRelMessage, performing the table scan
   */
  private static class ScanMapFunction implements MapFunction<SamzaSqlInputMessage, SamzaSqlRelMessage> {
    // All the user-supplied functions are expected to be serializable in order to enable full serialization of user
    // DAG. We do not want to serialize samzaMsgConverter as it can be fully constructed during stream operator
    // initialization.
    private transient SamzaRelConverter msgConverter;
    private transient MetricsRegistry metricsRegistry;
    private transient SamzaHistogram processingTime; // milli-seconds
    private transient Counter queryInputEvents;

    private final String streamName;
    private final int queryId;
    private final String queryLogicalId;
    private final String logicalOpId;

    ScanMapFunction(String sourceStreamName, int queryId, String queryLogicalId, String logicalOpId) {
      this.streamName = sourceStreamName;
      this.queryId = queryId;
      this.queryLogicalId = queryLogicalId;
      this.logicalOpId = logicalOpId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.msgConverter = translatorContext.getMsgConverter(streamName);
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      processingTime = new SamzaHistogram(metricsRegistry, logicalOpId, TranslatorConstants.PROCESSING_TIME_NAME);
      queryInputEvents = metricsRegistry.newCounter(queryLogicalId, TranslatorConstants.INPUT_EVENTS_NAME);
      queryInputEvents.clear();
    }

    @Override
    public SamzaSqlRelMessage apply(SamzaSqlInputMessage samzaSqlInputMessage) {
      long startProcessingMs = System.currentTimeMillis();
      long startProcessingNs = System.nanoTime();
      /* SAMZA-2089/LISAMZA-10654: the SamzaRelConverter.convertToRelMessage currently does not initialize
       *                           the samzaSqlRelMessage.samzaSqlRelMsgMetadata, this needs to be fixed */
      SamzaSqlRelMessage retMsg = this.msgConverter.convertToRelMessage(samzaSqlInputMessage.getKeyAndMessageKV());
      retMsg.setEventTime(samzaSqlInputMessage.getMetadata().getEventTime());
      retMsg.setArrivalTime(samzaSqlInputMessage.getMetadata().getArrivalTime());
      retMsg.setScanTime(startProcessingNs, startProcessingMs);
      updateMetrics(startProcessingNs, System.nanoTime());
      return retMsg;
    }

    /**
     * Updates the MetricsRegistery of this operator
     * @param startProcessingNs = begin processing of the message
     * @param endProcessing = end of processing
     */
    private void updateMetrics(long startProcessingNs, long endProcessing) {
      queryInputEvents.inc();
      processingTime.update(endProcessing - startProcessingNs);
    }
  } // ScanMapFunction

  void translate(final TableScan tableScan, final String queryLogicalId, final String logicalOpId,
      final TranslatorContext context, Map<String, DelegatingSystemDescriptor> systemDescriptors,
      Map<String, MessageStream<SamzaSqlInputMessage>> inputMsgStreams) {
    StreamApplicationDescriptor streamAppDesc = context.getStreamAppDescriptor();
    List<String> tableNameParts = tableScan.getTable().getQualifiedName();
    String sourceName = SqlIOConfig.getSourceFromSourceParts(tableNameParts);

    Validate.isTrue(relMsgConverters.containsKey(sourceName), String.format("Unknown source %s", sourceName));
    SqlIOConfig sqlIOConfig = systemStreamConfig.get(sourceName);
    final String systemName = sqlIOConfig.getSystemName();
    final String streamId = sqlIOConfig.getStreamId();
    final String source = sqlIOConfig.getSource();

    final boolean isRemoteTable = sqlIOConfig.getTableDescriptor().isPresent() && (
        sqlIOConfig.getTableDescriptor().get() instanceof RemoteTableDescriptor || sqlIOConfig.getTableDescriptor()
            .get() instanceof CachingTableDescriptor);

    // For remote table, we don't have an input stream descriptor. The table descriptor is already defined by the
    // SqlIOResolverFactory.
    // For local table, even though table descriptor is already defined, we still need to create the input stream
    // descriptor to load the local table.
    if (isRemoteTable) {
      return;
    }

    // set the wrapper input transformer (SamzaSqlInputTransformer) in system descriptor
    DelegatingSystemDescriptor systemDescriptor = systemDescriptors.get(systemName);
    if (systemDescriptor == null) {
      systemDescriptor = new DelegatingSystemDescriptor(systemName, new SamzaSqlInputTransformer());
      systemDescriptors.put(systemName, systemDescriptor);
    } else {
      /* in SamzaSQL, there should be no systemDescriptor setup by user, so this branch happens only
       * in case of Fan-OUT (i.e., same input stream used in multiple sql statements), or when same input
       * used twice in same sql statement (e.g., select ... from input as i1, input as i2 ...), o.w., throw error */
      if (systemDescriptor.getTransformer().isPresent()) {
        InputTransformer existingTransformer = systemDescriptor.getTransformer().get();
        if (!(existingTransformer instanceof SamzaSqlInputTransformer)) {
          throw new SamzaException(
              "SamzaSQL Exception: existing transformer for " + systemName + " is not SamzaSqlInputTransformer");
        }
      }
    }

    InputDescriptor inputDescriptor = systemDescriptor.getInputDescriptor(streamId, new NoOpSerde<>());

    if (!inputMsgStreams.containsKey(source)) {
      MessageStream<SamzaSqlInputMessage> inputMsgStream = streamAppDesc.getInputStream(inputDescriptor);
      inputMsgStreams.put(source, inputMsgStream.map(new SystemMessageMapperFunction(source, queryId)));
    }
    MessageStream<SamzaSqlRelMessage> samzaSqlRelMessageStream = inputMsgStreams.get(source)
        .filter(new FilterSystemMessageFunction(sourceName, queryId))
        .map(new ScanMapFunction(sourceName, queryId, queryLogicalId, logicalOpId));

    context.registerMessageStream(tableScan.getId(), samzaSqlRelMessageStream);
  }

  /**
   * Function that populates whether the message is a system message.
   * TODO This should ideally be populated by the InputTransformer in future.
   */
  private static class SystemMessageMapperFunction implements MapFunction<SamzaSqlInputMessage, SamzaSqlInputMessage> {
    private final String source;
    private final int queryId;
    private transient SamzaRelConverter relConverter;

    public SystemMessageMapperFunction(String source, int queryId) {
      this.source = source;
      this.queryId = queryId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      relConverter = translatorContext.getMsgConverter(source);
    }

    @Override
    public SamzaSqlInputMessage apply(SamzaSqlInputMessage message) {
      message.getMetadata().setIsSystemMessage(relConverter.isSystemMessage(message.getKeyAndMessageKV()));
      return message;
    }
  }
}
