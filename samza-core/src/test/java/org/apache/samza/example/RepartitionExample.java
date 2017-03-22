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
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.function.Supplier;


/**
 * Example {@link StreamApplication} code to test the API methods with re-partition operator
 */
public class RepartitionExample implements StreamApplication {

  @Override public void init(StreamGraph graph, Config config) {
    Supplier<Integer> initialValue = () -> 0;
    MessageStream<PageViewEvent> pageViewEvents =
        graph.getInputStream("pageViewEventStream", (k, m) -> (PageViewEvent) m);

    pageViewEvents
        .partitionBy(m -> m.memberId)
        .window(Windows.keyedTumblingWindow(m -> m.memberId, Duration.ofMinutes(5), initialValue, (m, c) -> c + 1))
        .map(MyStreamOutput::new)
        .sendTo("pageViewEventPerMemberStream", m -> m.memberId);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    // for remote execution: ApplicationRunner runner = ApplicationRunner.getRemoteRunner(config);
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new RepartitionExample());
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

  class MyStreamOutput {
    String memberId;
    long timestamp;
    int count;

    MyStreamOutput(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }
  }
}
