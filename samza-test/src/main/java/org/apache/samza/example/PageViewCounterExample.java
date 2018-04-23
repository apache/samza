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

import com.google.common.cache.Cache;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SupplierFunction;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import sun.net.www.http.HttpClient;
import sun.util.locale.LocaleObjectCache;


/**
 * Example code to implement window-based counter
 */
public class PageViewCounterExample implements StreamApplication {

  static class MyFilterFunction implements FilterFunction<PageViewEvent> {
    private transient Map<String, String> cachePerTask;

    @Override
    public boolean apply(PageViewEvent message) {
      return checkDups(message);
    }

    private boolean checkDups(PageViewEvent message) {
      return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void init(Config config, TaskContext context) {
      this.cachePerTask = (Map<String, String>) context.getUserContext();
    }
  }

  // local execution mode
  public static void main(String[] args) throws IOException {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    PageViewCounterExample app = new PageViewCounterExample();
    LocalApplicationRunner runner = new LocalApplicationRunner(config);

    runner.run(app);
    runner.waitForFinish();
  }

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<PageViewEvent> pageViewEvents = null;
    pageViewEvents = graph.getInputStream("pageViewEventStream", new JsonSerdeV2<>(PageViewEvent.class));
    OutputStream<KV<String, PageViewCount>> pageViewEventPerMemberStream =
        graph.getOutputStream("pageViewEventPerMemberStream",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageViewCount.class)));

    SupplierFunction<Integer> initialValue = () -> 0;
    FoldLeftFunction<PageViewEvent, Integer> foldLeftFn = (m, c) -> c + 1;
    pageViewEvents
        .window(Windows.keyedTumblingWindow(m -> m.memberId, Duration.ofSeconds(10), initialValue, foldLeftFn, null, null)
            .setEarlyTrigger(Triggers.repeat(Triggers.count(5)))
            .setAccumulationMode(AccumulationMode.DISCARDING), "tumblingWindow")
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), new PageViewCount(windowPane)))
        .sendTo(pageViewEventPerMemberStream);

    graph.withContextManager(new ContextManager() {
      @Override
      public void init(Config config, TaskContext context) {
        context.setUserContext(new ConcurrentHashMap<>());
      }

      @Override
      public void close() {

      }
    });

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

    PageViewCount(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }
  }
}
