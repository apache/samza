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

import com.google.common.collect.ImmutableList;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.config.Config;
import org.apache.samza.system.kafka.KafkaSystem;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;

public class MergeExample {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9092")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor.Input<String, PageViewEvent> input1 = StreamDescriptor.<String, PageViewEvent>input("viewStream1")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Input<String, PageViewEvent> input2 = StreamDescriptor.<String, PageViewEvent>input("viewStream2")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Input<String, PageViewEvent> input3 = StreamDescriptor.<String, PageViewEvent>input("viewStream3")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Output<String, PageViewEvent> output = StreamDescriptor.<String, PageViewEvent>output("mergedStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    StreamApplication app = StreamApplications.createStreamApp(config);
    MessageStream.mergeAll(ImmutableList.of(app.openInput(input1), app.openInput(input2), app.openInput(input3)))
        .sendTo(app.openOutput(output, m -> m.pageId));

    app.run();
    app.waitForFinish();
  }

  class PageViewEvent {
    String pageId;
    long viewTimestamp;
  }
}