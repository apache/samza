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
import org.apache.commons.lang.Validate;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;


/**
 * Translator to translate the TableScans in relational graph to the corresponding input streams in the StreamGraph
 * implementation
 */
class ScanTranslator {

  private final Map<String, SamzaRelConverter> relMsgConverters;
  private final Map<String, SqlIOConfig> systemStreamConfig;
  private final int queryId;

  ScanTranslator(Map<String, SamzaRelConverter> converters, Map<String, SqlIOConfig> ssc, int queryId) {
    relMsgConverters = converters;
    this.systemStreamConfig = ssc;
    this.queryId = queryId;
  }

  private static class ScanMapFunction implements MapFunction<KV<Object, Object>, SamzaSqlRelMessage> {
    // All the user-supplied functions are expected to be serializable in order to enable full serialization of user
    // DAG. We do not want to serialize samzaMsgConverter as it can be fully constructed during stream operator
    // initialization.
    private transient SamzaRelConverter msgConverter;
    private final String streamName;
    private final int queryId;

    ScanMapFunction(String sourceStreamName, int queryId) {
      this.streamName = sourceStreamName;
      this.queryId = queryId;
    }

    @Override
    public void init(Context context) {
      TranslatorContext translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.msgConverter = translatorContext.getMsgConverter(streamName);
    }

    @Override
    public SamzaSqlRelMessage apply(KV<Object, Object> message) {
      return this.msgConverter.convertToRelMessage(message);
    }
  }

  void translate(final TableScan tableScan, final TranslatorContext context,
      Map<String, DelegatingSystemDescriptor> systemDescriptors, Map<String, MessageStream<KV<Object, Object>>> inputMsgStreams) {
    StreamApplicationDescriptor streamAppDesc = context.getStreamAppDescriptor();
    List<String> tableNameParts = tableScan.getTable().getQualifiedName();
    String sourceName = SqlIOConfig.getSourceFromSourceParts(tableNameParts);

    Validate.isTrue(relMsgConverters.containsKey(sourceName), String.format("Unknown source %s", sourceName));
    SqlIOConfig sqlIOConfig = systemStreamConfig.get(sourceName);
    final String systemName = sqlIOConfig.getSystemName();
    final String streamId = sqlIOConfig.getStreamId();
    final String source = sqlIOConfig.getSource();

    final boolean isRemoteTable = sqlIOConfig.getTableDescriptor().isPresent() &&
        (sqlIOConfig.getTableDescriptor().get() instanceof RemoteTableDescriptor);

    // For remote table, we don't have an input stream descriptor. The table descriptor is already defined by the
    // SqlIOResolverFactory.
    // For local table, even though table descriptor is already defined, we still need to create the input stream
    // descriptor to load the local table.
    if (isRemoteTable) {
      return;
    }

    KVSerde<Object, Object> noOpKVSerde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    DelegatingSystemDescriptor
        sd = systemDescriptors.computeIfAbsent(systemName, DelegatingSystemDescriptor::new);
    GenericInputDescriptor<KV<Object, Object>> isd = sd.getInputDescriptor(streamId, noOpKVSerde);

    MessageStream<KV<Object, Object>> inputStream = inputMsgStreams.computeIfAbsent(source, v -> streamAppDesc.getInputStream(isd));
    MessageStream<SamzaSqlRelMessage> samzaSqlRelMessageStream = inputStream.map(new ScanMapFunction(sourceName, queryId));
    context.registerMessageStream(tableScan.getId(), samzaSqlRelMessageStream);
  }
}
