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

import java.time.Duration;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;


/**
 * Example {@link StreamApplication} code to test the API methods with re-partition operator
 */
public class RepartitionExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new RepartitionExample(), config);

    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    KafkaSystemDescriptor trackingSystem = new KafkaSystemDescriptor("tracking");

    KafkaInputDescriptor<PageViewEvent> inputStreamDescriptor =
        trackingSystem.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));

    KafkaOutputDescriptor<KV<String, MyStreamOutput>> outputStreamDescriptor =
        trackingSystem.getOutputDescriptor("pageViewEventPerMember",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(MyStreamOutput.class)));

    appDesc.withDefaultSystem(trackingSystem, null); // serde must be specified explicitly for intermediate streams
    MessageStream<PageViewEvent> pageViewEvents = appDesc.getInputStream(inputStreamDescriptor);
    OutputStream<KV<String, MyStreamOutput>> pageViewEventPerMember = appDesc.getOutputStream(outputStreamDescriptor);

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
