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
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
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
 * Example implementation of a task that splits its input into multiple output streams.
 */
public class BroadcastExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new BroadcastExample(), config);
    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    KVSerde<String, PageViewEvent> serde = KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageViewEvent.class));
    KafkaSystemDescriptor trackingSystem = new KafkaSystemDescriptor("tracking");
    KafkaInputDescriptor<KV<String, PageViewEvent>> pageViewEvent =
        trackingSystem.getInputDescriptor("pageViewEvent", serde);
    KafkaOutputDescriptor<KV<String, PageViewEvent>> outStream1 =
        trackingSystem.getOutputDescriptor("outStream1", serde);
    KafkaOutputDescriptor<KV<String, PageViewEvent>> outStream2 =
        trackingSystem.getOutputDescriptor("outStream2", serde);
    KafkaOutputDescriptor<KV<String, PageViewEvent>> outStream3 =
        trackingSystem.getOutputDescriptor("outStream3", serde);

    MessageStream<KV<String, PageViewEvent>> inputStream = appDesc.getInputStream(pageViewEvent);
    inputStream.filter(m -> m.key.equals("key1")).sendTo(appDesc.getOutputStream(outStream1));
    inputStream.filter(m -> m.key.equals("key2")).sendTo(appDesc.getOutputStream(outStream2));
    inputStream.filter(m -> m.key.equals("key3")).sendTo(appDesc.getOutputStream(outStream3));
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
