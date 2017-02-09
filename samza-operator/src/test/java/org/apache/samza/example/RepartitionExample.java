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

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.*;


/**
 * Example {@link StreamGraphFactory} code to test the API methods with re-partition operator
 */
public class RepartitionExample implements StreamGraphFactory {

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.getRemoteEnvironment(config);
   *     remoteEnv.run(new UserMainExample(), config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    MessageStream<PageViewEvent> pageViewEvents = graph.createInStream(input1, new StringSerde("UTF-8"), new JsonSerde<>());
    OutputStream<MyStreamOutput> pageViewPerMemberCounters = graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>());

    pageViewEvents.
        partitionBy(m -> m.getMessage().memberId).
        window(Windows.<PageViewEvent, String, Integer>keyedTumblingWindow(
            msg -> msg.getMessage().memberId, Duration.ofMinutes(5), (m, c) -> c + 1)).
        map(MyStreamOutput::new).
        sendTo(pageViewPerMemberCounters);

    return graph;
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new RepartitionExample(), config);
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
