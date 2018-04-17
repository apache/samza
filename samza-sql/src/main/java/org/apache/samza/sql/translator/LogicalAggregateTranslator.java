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
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;


/**
 * Translator to translate the LogicalAggregate node in the relational graph to the corresponding StreamGraph
 * implementation
 */
class LogicalAggregateTranslator {

  private int windowId;

  LogicalAggregateTranslator(int windowId) {
    this.windowId = windowId;
  }

  void translate(final LogicalAggregate aggregate, final TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> inputStream = context.getMessageStream(aggregate.getInput().getId());

    Supplier<Long> initialValue = () -> (long) 0;
    FoldLeftFunction<SamzaSqlRelMessage, Long> foldLeftFn = (m, c) -> c + 1;

    MessageStream<SamzaSqlRelMessage> outputStream =
        inputStream
            .window(Windows.keyedTumblingWindow(m -> m,
                Duration.ofMillis(context.getExecutionContext().getSamzaSqlApplicationConfig().getWindowDurationMs()),
                initialValue,
                foldLeftFn,
                new SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde(),
                new LongSerde())
                .setAccumulationMode(AccumulationMode.DISCARDING), "tumblingWindow_" + windowId)
            .map(windowPane -> {
              List<String> fieldNames = windowPane.getKey().getKey().getSamzaSqlRelRecord().getFieldNames();
              List<Object> fieldValues = windowPane.getKey().getKey().getSamzaSqlRelRecord().getFieldValues();
              fieldNames.add(aggregate.getAggCallList().get(0).getName());
              fieldValues.add(windowPane.getMessage());
              return new SamzaSqlRelMessage(fieldNames, fieldValues);
            });
    context.registerMessageStream(aggregate.getId(), outputStream);
  }
}
