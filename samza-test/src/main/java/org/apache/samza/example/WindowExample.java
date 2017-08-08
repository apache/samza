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
import org.apache.samza.application.StreamApplications;
import org.apache.samza.config.Config;
import org.apache.samza.system.kafka.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;

import java.time.Duration;


/**
 * Example implementation of a simple user-defined task w/ a window operator.
 *
 */
public class WindowExample {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9092")
        .withProducerProperties(config)
        .withConsumerProperties(config);

    StreamDescriptor.Input<String, PageViewEvent> input = StreamDescriptor.<String, PageViewEvent>input("pageViewEvent")
        .withKeySerde(new StringSerde("UFT-8"))
        .withKeySerde(new JsonSerde<>())
        .from(kafkaSystem);

    StreamDescriptor.Output<String, Integer> output = StreamDescriptor.<String, Integer>output("pageViewEventWindowedCounter")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new IntegerSerde())
        .from(kafkaSystem);

    StreamApplication app = StreamApplications.createStreamApp(config);

    app.openInput(input)
        .window(Windows.<PageViewEvent, Integer>tumblingWindow(Duration.ofMinutes(10), () -> 0, (m, c) -> c + 1)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceLastMessage(Duration.ofMinutes(1)))))
        .sendTo(app.openOutput(output, m -> m.getKey().getPaneId(), m -> m.getMessage()));

    app.run();
    app.waitForFinish();
  }

  class PageViewEvent {
    String key;
    long timestamp;

    public PageViewEvent(String key, long timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }
  }
}
