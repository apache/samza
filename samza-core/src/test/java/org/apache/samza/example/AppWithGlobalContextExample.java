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

import com.sun.org.glassfish.external.statistics.Stats;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.CommandLine;

/**
 * Example code using {@link KeyValueStore} to implement event-time window
 */
public class AppWithGlobalContextExample {

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
    StreamDescriptor.Output<String, StatsOutput> statsStream = StreamDescriptor.<String, StatsOutput>output("pageViewEventStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    CloseableHttpClient client = HttpClientBuilder.create().build();

    StreamApplication app = StreamApplication.create(config).withContextManager(new ContextManager() {

      @Override
      public void init(Config config, TaskContext context) {
        context.setUserContext(client);
      }

      @Override
      public void close() {
        try {
          client.close();
        } catch (IOException e) {
          throw new RuntimeException("HttpClient IOException.", e);
        }
      }
    });

    app.open(pageViewEventInput)
        .partitionBy(m -> m.memberId)
        .flatMap(new MyStatsCounter())
        .sendTo(app.open(statsStream, m -> m.memberId));

    app.run();
    app.waitForFinish();
  }

  static class MyStatsCounter implements FlatMapFunction<PageViewEvent, StatsOutput> {
    private final int timeoutMs = 10 * 60 * 1000;
    private CloseableHttpClient storeClient;

    class StatsWindowState {
      int lastCount = 0;
      long timeAtLastOutput = 0;
      int newCount = 0;
    }

    StatsWindowState getWindowState(String wndKey) {
      try {
        HttpGet getWndState = new HttpGet(String.format("http://remote-store/%s", wndKey));
        CloseableHttpResponse response = this.storeClient.execute(getWndState);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
          result.append(line);
        }
        return new JsonSerde<StatsWindowState>().fromBytes(result.toString().getBytes());
      } catch (IOException e) {
        throw new RuntimeException("IOException while getting window state from remote store", e);
      }
    }

    void putWindowState(String wndKey, StatsWindowState state) {
      try {
        HttpPost postWndState = new HttpPost(String.format("http://remote-store/%s", wndKey));
        postWndState.setEntity(new ByteArrayEntity(new JsonSerde<StatsWindowState>().toBytes(state)));

        CloseableHttpResponse response = this.storeClient.execute(postWndState);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
          throw new RuntimeException("Error in posting window state. Status code: " + response.getStatusLine().getStatusCode());
        }
      } catch (IOException e) {
        throw new RuntimeException("IOException while getting window state from remote store", e);
      }
    }

    @Override
    public Collection<StatsOutput> apply(PageViewEvent message) {
      List<StatsOutput> outputStats = new ArrayList<>();
      long wndTimestamp = (long) Math.floor(TimeUnit.MILLISECONDS.toMinutes(message.timestamp) / 5) * 5;
      String wndKey = String.format("%s-%d", message.memberId, wndTimestamp);
      StatsWindowState curState = getWindowState(wndKey);
      curState.newCount++;
      long curTimeMs = System.currentTimeMillis();
      if (curState.newCount > 0 && curState.timeAtLastOutput + timeoutMs < curTimeMs) {
        curState.timeAtLastOutput = curTimeMs;
        curState.lastCount += curState.newCount;
        curState.newCount = 0;
        outputStats.add(new StatsOutput(message.memberId, wndTimestamp, curState.lastCount));
      }
      // update counter w/o generating output
      putWindowState(wndKey, curState);
      return outputStats;
    }

    @Override
    public void init(Config config, TaskContext context) {
      this.storeClient = (CloseableHttpClient) context.getUserContext();
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
