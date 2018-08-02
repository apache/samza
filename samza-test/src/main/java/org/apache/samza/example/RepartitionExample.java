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
package org.apache.samza.example;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;

import java.time.Duration;


/**
 * Example {@link StreamApplication} code to test the API methods with re-partition operator
 */
public class RepartitionExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    RepartitionExample app = new RepartitionExample();
    LocalApplicationRunner runner = new LocalApplicationRunner(config);

    runner.run(app);
    runner.waitForFinish();
  }

  @Override
  public void init(StreamGraph graph, Config config) {
    KafkaSystemDescriptor<KV<Object, Object>> trackingSystem =
        new KafkaSystemDescriptor<>("tracking", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));

    KafkaInputDescriptor<PageViewEvent> inputStreamDescriptor =
        trackingSystem.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));

    KafkaOutputDescriptor<KV<String, MyStreamOutput>> outputStreamDescriptor =
        trackingSystem.getOutputDescriptor("pageViewEventPerMember",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(MyStreamOutput.class)));

    graph.setDefaultSystem(trackingSystem);
    MessageStream<PageViewEvent> pageViewEvents = graph.getInputStream(inputStreamDescriptor);
    OutputStream<KV<String, MyStreamOutput>> pageViewEventPerMember = graph.getOutputStream(outputStreamDescriptor);

    pageViewEvents
        .partitionBy(pve -> pve.memberId, pve -> pve,
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageViewEvent.class)), "partitionBy")
        .window(Windows.keyedTumblingWindow(
            KV::getKey, Duration.ofMinutes(5), () -> 0, (m, c) -> c + 1, null, null), "window")
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), new MyStreamOutput(windowPane)))
        .sendTo(pageViewEventPerMember);

  }

  class PageViewEvent {
    String pageId;
    String memberId;
    long timestamp;

    PageViewEvent(String pageId, String memberId, long timestamp) {
      this.pageId = pageId;
      this.memberId = memberId;
      this.timestamp = timestamp;
    }
  }

  static class MyStreamOutput {
    String memberId;
    long timestamp;
    int count;

    MyStreamOutput(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }
  }
}
