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
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;


/**
 * Translator to translate the TableScans in relational graph to the corresponding input streams in the StreamGraph
 * implementation
 */
class ScanTranslator {

  private final Map<String, SamzaRelConverter> relMsgConverters;
  private final Map<String, SqlSystemSourceConfig> systemStreamConfig;

  ScanTranslator(Map<String, SamzaRelConverter> converters, Map<String, SqlSystemSourceConfig> ssc) {
    relMsgConverters = converters;
    this.systemStreamConfig = ssc;
  }

  void translate(final TableScan tableScan, final TranslatorContext context) {
    StreamGraph streamGraph = context.getStreamGraph();
    List<String> tableNameParts = tableScan.getTable().getQualifiedName();
    String sourceName = SqlSystemSourceConfig.getSourceFromSourceParts(tableNameParts);

    Validate.isTrue(relMsgConverters.containsKey(sourceName), String.format("Unknown source %s", sourceName));
    SamzaRelConverter converter = relMsgConverters.get(sourceName);
    String streamName = systemStreamConfig.get(sourceName).getStreamName();

    MessageStream<KV<Object, Object>> inputStream = streamGraph.getInputStream(streamName);
    MessageStream<SamzaSqlRelMessage> samzaSqlRelMessageStream = inputStream.map(converter::convertToRelMessage);

    context.registerMessageStream(tableScan.getId(), samzaSqlRelMessageStream);
  }
}
