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
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.function.Supplier;


/**
 * Example code to implement window-based counter
 */
public class PageViewCounterExample implements StreamApplication {

  private final StreamSpec inputStreamSpec = new StreamSpec("pageViewEventStream", "PageViewEvent", "kafka");
  private final StreamSpec outputStreamSpec = new StreamSpec("pageViewEventPerMemberStream", "PageViewEventCountByMemberId", "kafka");

  @Override public void init(StreamGraph graph, Config config) {
    MessageStream<PageViewEvent> pageViewEvents = graph.createInStream(inputStreamSpec, (k, m) -> m,
        new StringSerde("UTF-8"), new JsonSerde<PageViewEvent>());
    MessageStream<PageViewCount> pageViewPerMemberCounters =
        graph.createOutStream(outputStreamSpec, m -> m.memberId, m -> m,
            new StringSerde("UTF-8"), new JsonSerde<PageViewCount>());

    Supplier<Integer> initialValue = () -> 0;
    pageViewEvents.
        window(Windows.<PageViewEvent, String, Integer>keyedTumblingWindow(m -> m.memberId, Duration.ofSeconds(10), initialValue, (m, c) -> c + 1)
            .setEarlyTrigger(Triggers.repeat(Triggers.count(5)))
            .setAccumulationMode(AccumulationMode.DISCARDING)).
        map(PageViewCount::new).
        sendTo(pageViewPerMemberCounters);
  }

  // local execution mode
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    // for remote execution: ApplicationRunner runner = ApplicationRunner.getRemoteRunner(config);
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new PageViewCounterExample());
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

  class PageViewCount {
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
