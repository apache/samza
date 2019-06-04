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
import java.util.HashMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.example.models.PageViewCount;
import org.apache.samza.example.models.PageViewEvent;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;


/**
 * Example code to implement window-based counter
 */
public class AppWithGlobalConfigExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new AppWithGlobalConfigExample(), config);

    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor trackingSystem = new KafkaSystemDescriptor("tracking");

    KafkaInputDescriptor<PageViewEvent> inputStreamDescriptor =
        trackingSystem.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));

    KafkaOutputDescriptor<KV<String, PageViewCount>> outputStreamDescriptor =
        trackingSystem.getOutputDescriptor("pageViewEventPerMember",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageViewCount.class)));

    appDescriptor.getInputStream(inputStreamDescriptor)
        .window(Windows.<PageViewEvent, String, Integer>keyedTumblingWindow(PageViewEvent::getMemberId, Duration.ofSeconds(10), () -> 0, (m, c) -> c + 1,
            null, null)
            .setEarlyTrigger(Triggers.repeat(Triggers.count(5)))
            .setAccumulationMode(AccumulationMode.DISCARDING), "window1")
        .map(m -> KV.of(m.getKey().getKey(), buildPageViewCount(m)))
        .sendTo(appDescriptor.getOutputStream(outputStreamDescriptor));

    appDescriptor.withMetricsReporterFactories(new HashMap<>());
  }

  static PageViewCount buildPageViewCount(WindowPane<String, Integer> windowPane) {
    String memberId = windowPane.getKey().getKey();
    long timestamp = Long.valueOf(windowPane.getKey().getPaneId());
    int count = windowPane.getMessage();

    return new PageViewCount(memberId, timestamp, count);
  }
}
