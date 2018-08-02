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
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;

public class MergeExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    MergeExample app = new MergeExample();
    LocalApplicationRunner runner = new LocalApplicationRunner(config);

    runner.run(app);
    runner.waitForFinish();
  }

  @Override
  public void init(StreamGraph graph, Config config) {
    KafkaSystemDescriptor<KV<String, PageViewEvent>> trackingSystem =
        new KafkaSystemDescriptor<>("tracking", KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageViewEvent.class)));

    KafkaInputDescriptor<KV<String, PageViewEvent>> isd1 =
        trackingSystem.getInputDescriptor("pageViewStream1");
    KafkaInputDescriptor<KV<String, PageViewEvent>> isd2 =
        trackingSystem.getInputDescriptor("pageViewStream2");
    KafkaInputDescriptor<KV<String, PageViewEvent>> isd3 =
        trackingSystem.getInputDescriptor("pageViewStream3");

    KafkaOutputDescriptor<KV<String, PageViewEvent>> osd =
        trackingSystem.getOutputDescriptor("mergedStream");

    MessageStream
        .mergeAll(ImmutableList.of(graph.getInputStream(isd1), graph.getInputStream(isd2), graph.getInputStream(isd3)))
        .sendTo(graph.getOutputStream(osd));
  }

  class PageViewEvent {
    String pageId;
    long viewTimestamp;
  }
}