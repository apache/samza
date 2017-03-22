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
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * Example {@link StreamApplication} code to test the API methods
 */
public class NoContextStreamExample implements StreamApplication {

  @Override public void init(StreamGraph graph, Config config) {
    MessageStream<PageViewEvent> inputStream1 = graph.getInputStream("pageViewEvent", (k, m) -> (PageViewEvent) m);
    MessageStream<RumLixEvent> inputStream2 = graph.getInputStream("rumLixEvent", (k, m) -> (RumLixEvent) m);

    inputStream1
        .join(inputStream2, new MyJoinFunction(), Duration.ofMinutes(1))
        .sendTo("joinedPageViewStream", m -> m);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    // for remote execution: ApplicationRunner runner = ApplicationRunner.getRemoteRunner(config);
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new NoContextStreamExample());
  }

  class MyJoinFunction implements JoinFunction<String, PageViewEvent, RumLixEvent, PageViewEvent> {
    @Override
    public PageViewEvent apply(PageViewEvent m1, RumLixEvent m2) {
      PageViewEvent newJoinMsg = new PageViewEvent();
      newJoinMsg.joinKey = m1.joinKey;
      newJoinMsg.joinFields.addAll(m1.joinFields);
      newJoinMsg.joinFields.addAll(m2.joinFields);
      return newJoinMsg;
    }

    @Override
    public String getFirstKey(PageViewEvent message) {
      return message.joinKey;
    }

    @Override
    public String getSecondKey(RumLixEvent message) {
      return message.joinKey;
    }
  }

  class PageViewEvent {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class RumLixEvent {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }
}
