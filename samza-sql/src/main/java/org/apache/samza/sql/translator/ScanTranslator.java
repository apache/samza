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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang.Validate;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;

import com.google.common.base.Joiner;
import org.apache.samza.system.SystemStream;


/**
 * Translator to translate the TableScans in relational graph to the corresponding input streams in the StreamGraph
 * implementation
 */
public class ScanTranslator {

  private final Map<SystemStream, SamzaRelConverter> relMsgConverters;

  public ScanTranslator(Map<SystemStream, SamzaRelConverter> converters) {
    relMsgConverters = converters;
  }

  public void translate(final TableScan tableScan, final TranslatorContext context) {
    StreamGraph streamGraph = context.getStreamGraph();
    List<String> tableNameParts = tableScan.getTable().getQualifiedName();
    Validate.isTrue(tableNameParts.size() == 2,
        String.format("table name %s is not of the format <SystemName>.<StreamName>",
            Joiner.on(".").join(tableNameParts)));

    String streamName = tableNameParts.get(1);
    String systemName = tableNameParts.get(0);
    SystemStream systemStream = new SystemStream(systemName, streamName);

    Validate.isTrue(relMsgConverters.containsKey(systemStream), String.format("Unknown system %s", systemName));
    SamzaRelConverter converter = relMsgConverters.get(systemStream);

    MessageStream<KV<Object, Object>> inputStream = streamGraph.getInputStream(streamName);
    MessageStream<SamzaSqlRelMessage> samzaSqlRelMessageStream = inputStream.map(converter::convertToRelMessage);

    context.registerMessageStream(tableScan.getId(), samzaSqlRelMessageStream);
  }
}
