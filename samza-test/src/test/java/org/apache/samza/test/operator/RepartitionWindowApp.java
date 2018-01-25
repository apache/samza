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

import java.io.IOException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * A {@link StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp implements StreamApplication {

  private static final Logger LOG = LoggerFactory.getLogger(RepartitionWindowApp.class);

  static final String INPUT_TOPIC = "page-views";
  static final String OUTPUT_TOPIC = "Result";

  public static void main(String[] args) throws IOException {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    RepartitionWindowApp reparApp = new RepartitionWindowApp();
    LocalApplicationRunner runner = new LocalApplicationRunner(config);

    runner.run(reparApp);
    runner.waitForFinish();
  }

  @Override
  public void init(StreamGraph graph, Config config) {

    KVSerde<String, PageView>
        pgeMsgSerde = KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageView.class));

    graph.getInputStream(INPUT_TOPIC, pgeMsgSerde)
        .map(KV::getValue)
        .partitionBy(PageView::getUserId, m -> m, pgeMsgSerde, "p1")
        .window(Windows.keyedSessionWindow(m -> m.getKey(), Duration.ofSeconds(3), () -> 0, (m, c) -> c + 1, new StringSerde("UTF-8"), new IntegerSerde()), "w1")
        .map(wp -> KV.of(wp.getKey().getKey().toString(), String.valueOf(wp.getMessage())))
        .sendTo(graph.getOutputStream(OUTPUT_TOPIC));

  }
}
