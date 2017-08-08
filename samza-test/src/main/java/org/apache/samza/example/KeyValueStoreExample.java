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
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.CommandLine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Example code using {@link KeyValueStore} to implement event-time window
 */
public class KeyValueStoreExample {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9092")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor.Input<String, PageViewEvent> pageViewEventInput = StreamDescriptor.<String, PageViewEvent>input("pageViewEventStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Output<String, StatsOutput> statsStream = StreamDescriptor.<String, StatsOutput>output("pageViewEventStatsStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    StreamApplication app = StreamApplications.createStreamApp(config);
    app.openInput(pageViewEventInput)
        .partitionBy(m -> m.memberId)
        .flatMap(new MyStatsCounter())
        .sendTo(app.openOutput(statsStream, m -> m.memberId));

    app.run();
    app.waitForFinish();
  }

  static class MyStatsCounter implements FlatMapFunction<PageViewEvent, StatsOutput> {
    private final int timeoutMs = 10 * 60 * 1000;

    KeyValueStore<String, StatsWindowState> statsStore;

    class StatsWindowState {
      int lastCount = 0;
      long timeAtLastOutput = 0;
      int newCount = 0;
    }

    @Override
    public Collection<StatsOutput> apply(PageViewEvent message) {
      List<StatsOutput> outputStats = new ArrayList<>();
      long wndTimestamp = (long) Math.floor(TimeUnit.MILLISECONDS.toMinutes(message.timestamp) / 5) * 5;
      String wndKey = String.format("%s-%d", message.memberId, wndTimestamp);
      StatsWindowState curState = this.statsStore.get(wndKey);
      if (curState == null) {
        curState = new StatsWindowState();
      }
      curState.newCount++;
      long curTimeMs = System.currentTimeMillis();
      if (curState.newCount > 0 && curState.timeAtLastOutput + timeoutMs < curTimeMs) {
        curState.timeAtLastOutput = curTimeMs;
        curState.lastCount += curState.newCount;
        curState.newCount = 0;
        outputStats.add(new StatsOutput(message.memberId, wndTimestamp, curState.lastCount));
      }
      // update counter w/o generating output
      this.statsStore.put(wndKey, curState);
      return outputStats;
    }

    @Override
    public void init(Config config, TaskContext context) {
      this.statsStore = (KeyValueStore<String, StatsWindowState>) context.getStore("my-stats-wnd-store");
    }
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

  static class StatsOutput {
    private String memberId;
    private long timestamp;
    private Integer count;

    StatsOutput(String key, long timestamp, Integer count) {
      this.memberId = key;
      this.timestamp = timestamp;
      this.count = count;
    }
  }
}
