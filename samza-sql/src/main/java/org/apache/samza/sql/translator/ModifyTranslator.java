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
import java.util.Optional;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.table.Table;
import org.apache.samza.task.TaskContext;


/**
 * Translator to translate the TableModify in relational graph to the corresponding output streams in the StreamGraph
 * implementation
 */
class ModifyTranslator {

  private final Map<String, SamzaRelConverter> relMsgConverters;
  private final Map<String, SqlIOConfig> systemStreamConfig;

  ModifyTranslator(Map<String, SamzaRelConverter> converters, Map<String, SqlIOConfig> ssc) {
    relMsgConverters = converters;
    this.systemStreamConfig = ssc;
  }

  private static class OutputMapFunction implements MapFunction<SamzaSqlRelMessage, KV<Object, Object>> {
    private transient SamzaRelConverter samzaMsgConverter;
    private final String outputTopic;

    OutputMapFunction(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    @Override
    public void init(Config config, TaskContext taskContext) {
      TranslatorContext context = (TranslatorContext) taskContext.getUserContext();
      this.samzaMsgConverter = context.getMsgConverter(outputTopic);
    }

    @Override
    public KV<Object, Object> apply(SamzaSqlRelMessage message) {
      return this.samzaMsgConverter.convertToSamzaMessage(message);
    }
  }

  void translate(final TableModify tableModify, final TranslatorContext context) {
    StreamGraph streamGraph = context.getStreamGraph();
    List<String> tableNameParts = tableModify.getTable().getQualifiedName();
    String targetName = SqlIOConfig.getSourceFromSourceParts(tableNameParts);

    Validate.isTrue(relMsgConverters.containsKey(targetName), String.format("Unknown source %s", targetName));

    SqlIOConfig sinkConfig = systemStreamConfig.get(targetName);
    MessageStreamImpl<SamzaSqlRelMessage> stream =
        (MessageStreamImpl<SamzaSqlRelMessage>) context.getMessageStream(tableModify.getInput().getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(new OutputMapFunction(targetName));

    Optional<TableDescriptor> tableDescriptor = sinkConfig.getTableDescriptor();
    if (!tableDescriptor.isPresent()) {
      outputStream.sendTo(streamGraph.getOutputStream(sinkConfig.getStreamName()));
    } else {
      Table outputTable = streamGraph.getTable(tableDescriptor.get());
      if (outputTable == null) {
        String msg = "Failed to obtain table descriptor of " + sinkConfig.getSource();
        throw new SamzaException(msg);
      }
      outputStream.sendTo(outputTable);
    }
  }
}
