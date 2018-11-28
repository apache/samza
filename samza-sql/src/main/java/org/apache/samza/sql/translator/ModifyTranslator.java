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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.core.TableModify;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;


/**
 * Translator to translate the TableModify in relational graph to the corresponding output streams in the StreamGraph
 * implementation
 */
class ModifyTranslator {

  private final Map<String, SamzaRelConverter> relMsgConverters;
  private final Map<String, SqlIOConfig> systemStreamConfig;
  private final int queryId;

  ModifyTranslator(Map<String, SamzaRelConverter> converters, Map<String, SqlIOConfig> ssc, int queryId) {
    relMsgConverters = converters;
    this.systemStreamConfig = ssc;
    this.queryId = queryId;
  }

  // OutputMapFunction converts SamzaSqlRelMessage to SamzaMessage in KV format
  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    // All the user-supplied functions are expected to be serializable in order to enable full serialization of user
    // DAG. We do not want to serialize samzaMsgConverter as it can be fully constructed during stream operator
    // initialization.
    private transient SamzaRelConverter samzaMsgConverter;
    private transient MetricsRegistry metricsRegistry;
    private transient SamzaHistogram processingTime; // milli-seconds
    private transient Counter inputEvents;

    private final String outputTopic;
    private final int queryId;
    private final String logicalOpId;

    OutputMapFunction(String outputTopic, int queryId, String logicalOpId) {
      this.outputTopic = outputTopic;
      this.queryId = queryId;
      this.logicalOpId = logicalOpId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.samzaMsgConverter = translatorContext.getMsgConverter(outputTopic);
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      processingTime = new SamzaHistogram(metricsRegistry, logicalOpId, TranslatorConstants.PROCESSING_TIME_NAME);
      inputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.INPUT_EVENTS_NAME);
      inputEvents.clear();

    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      Instant startProcessing = Instant.now();
      KV<Object, Object> retKV = this.samzaMsgConverter.convertToSamzaMessage(message);
      updateMetrics(startProcessing, Instant.now());
      return retKV;

    }

    /**
     * Updates the MetricsRegistery of this operator
     * @param startProcessing = begin processing of the message
     * @param endProcessing = end of processing
     */
    private void updateMetrics(Instant startProcessing, Instant endProcessing) {
      inputEvents.inc();
      processingTime.update(Duration.between(startProcessing, endProcessing).toMillis());
    }

  }

  void translate(String logicalOpId, final TableModify tableModify, final TranslatorContext context, Map<String, DelegatingSystemDescriptor> systemDescriptors,
      Map<String, OutputStream> outputMsgStreams) {

    StreamApplicationDescriptor streamAppDesc = context.getStreamAppDescriptor();
    List<String> tableNameParts = tableModify.getTable().getQualifiedName();
    String targetName = SqlIOConfig.getSourceFromSourceParts(tableNameParts);

    Validate.isTrue(relMsgConverters.containsKey(targetName), String.format("Unknown source %s", targetName));

    SqlIOConfig sinkConfig = systemStreamConfig.get(targetName);

    final String systemName = sinkConfig.getSystemName();
    final String streamId = sinkConfig.getStreamId();
    final String source = sinkConfig.getSource();

    KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    DelegatingSystemDescriptor
        sd = systemDescriptors.computeIfAbsent(systemName, DelegatingSystemDescriptor::new);
    GenericOutputDescriptor<KV<Object, Object>> osd = sd.getOutputDescriptor(streamId, noOpKVSerde);

    MessageStreamImpl<SamzaSqlRelMessage> stream =
        (MessageStreamImpl<SamzaSqlRelMessage>) context.getMessageStream(tableModify.getInput().getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(targetName, queryId, logicalOpId));

    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      OutputStream stm = outputMsgStreams.computeIfAbsent(source, v -> streamAppDesc.getOutputStream(osd));
      outputStream.sendTo(stm);
    } else {
      Table outputTable = streamAppDesc.getTable(tableDescriptor.get());
      if (outputTable == null) {
        String msg = "Failed to obtain table descriptor of " + sinkConfig.getSource();
        throw new SamzaException(msg);
      }
      outputStream.sendTo(outputTable);
    }
  }
}
