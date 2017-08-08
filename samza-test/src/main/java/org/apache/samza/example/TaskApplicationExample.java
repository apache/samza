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

import org.apache.samza.application.StreamApplications;
import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.system.kafka.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.util.CommandLine;

import java.util.Collections;


public class TaskApplicationExample {

  static class MyStreamTaskFactory implements StreamTaskFactory {

    @Override
    public StreamTask createInstance() {
      return (envelope, collector, coordinator) -> {
        return;
      };
    }
  }

  // local execution mode
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9192")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor.Input<String, PageViewEvent> input = StreamDescriptor.<String, PageViewEvent>input("myPageViewEvent")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Output<String, PageViewCount> output = StreamDescriptor.<String, PageViewCount>output("pageViewEventPerMemberStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    StreamTaskApplication app = StreamApplications.createStreamTaskApp(config, new MyStreamTaskFactory());

    app.addInputs(Collections.singletonList(input)).addOutputs(Collections.singletonList(output)).run();
    app.waitForFinish();
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

  static class PageViewCount {
    String memberId;
    long timestamp;
    int count;

    PageViewCount(String memberId, long timestamp, int count) {
      this.memberId = memberId;
      this.timestamp = timestamp;
      this.count = count;
    }
  }

}
