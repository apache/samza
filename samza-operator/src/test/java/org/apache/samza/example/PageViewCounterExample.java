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

import org.apache.samza.operators.*;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.Properties;


/**
 * Example code to implement window-based counter
 */
public class PageViewCounterExample implements StreamGraphBuilder {

  @Override public void init(StreamGraph graph, Config config) {

    MessageStream<PageViewEvent> pageViewEvents = graph.createInStream(input1, new StringSerde("UTF-8"), new JsonSerde<>());
    OutputStream<MyStreamOutput> pageViewPerMemberCounters = graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>());

    pageViewEvents.
        window(Windows.<PageViewEvent, String, Integer>keyedTumblingWindow(m -> m.getMessage().memberId, Duration.ofSeconds(10), (m, c) -> c + 1).
            setEarlyTrigger(Triggers.repeat(Triggers.count(5))).
            setAccumulationMode(AccumulationMode.DISCARDING)).
        map(MyStreamOutput::new).
        sendTo(pageViewPerMemberCounters);

  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new PageViewCounterExample(), config);
  }

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "PageViewEvent");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "PageViewPerMember5min");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  class PageViewEvent implements MessageEnvelope<String, PageViewEvent> {
    String pageId;
    String memberId;
    long timestamp;

    PageViewEvent(String pageId, String memberId, long timestamp) {
      this.pageId = pageId;
      this.memberId = memberId;
      this.timestamp = timestamp;
    }

    @Override
    public String getKey() {
      return this.pageId;
    }

    @Override
    public PageViewEvent getMessage() {
      return this;
    }
  }

  class MyStreamOutput implements MessageEnvelope<String, MyStreamOutput> {
    String memberId;
    long timestamp;
    int count;

    MyStreamOutput(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }

    @Override
    public String getKey() {
      return this.memberId;
    }

    @Override
    public MyStreamOutput getMessage() {
      return this;
    }
  }

}
