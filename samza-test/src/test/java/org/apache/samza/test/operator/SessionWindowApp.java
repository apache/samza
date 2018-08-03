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

package org.apache.samza.test.operator;

import java.time.Duration;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.util.CommandLine;

/**
 * A {@link StreamApplication} that demonstrates a filter followed by a session window.
 */
public class SessionWindowApp implements StreamApplication {
  private static final String SYSTEM = "kafka";
  private static final String INPUT_TOPIC = "page-views";
  private static final String OUTPUT_TOPIC = "page-view-counts";
  private static final String FILTER_KEY = "badKey";

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    SessionWindowApp app = new SessionWindowApp();
    LocalApplicationRunner runner = new LocalApplicationRunner(config);

    runner.run(app);
    runner.waitForFinish();
  }

  @Override
  public void init(StreamGraph graph, Config config) {
    JsonSerdeV2<PageView> inputSerde = new JsonSerdeV2<>(PageView.class);
    KVSerde<String, Integer> outputSerde = KVSerde.of(new StringSerde(), new IntegerSerde());
    KafkaSystemDescriptor<Object> ksd = new KafkaSystemDescriptor<>(SYSTEM);
    KafkaInputDescriptor<PageView> id = ksd.getInputDescriptor(INPUT_TOPIC, inputSerde);
    KafkaOutputDescriptor<KV<String, Integer>> od = ksd.getOutputDescriptor(OUTPUT_TOPIC, outputSerde);

    MessageStream<PageView> pageViews = graph.getInputStream(id);
    OutputStream<KV<String, Integer>> outputStream = graph.getOutputStream(od);

    pageViews
        .filter(m -> !FILTER_KEY.equals(m.getUserId()))
        .window(Windows.keyedSessionWindow(PageView::getUserId, Duration.ofSeconds(3),
            new StringSerde(), new JsonSerdeV2<>(PageView.class)), "sessionWindow")
        .map(m -> KV.of(m.getKey().getKey(), m.getMessage().size()))
        .sendTo(outputStream);
  }
}
