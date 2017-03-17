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
import org.apache.samza.operators.data.JsonMessage;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * Example {@link StreamApplication} code to test the API methods
 */
public class NoContextStreamExample implements StreamApplication {

  private final StreamSpec inputSpec1 = new StreamSpec("inputStreamA", "PageViewEvent", "kafka");
  private final StreamSpec inputSpec2 = new StreamSpec("inputStreamB", "RumLixEvent", "kafka");
  private final StreamSpec outputSpec = new StreamSpec("joinedPageViewStream", "PageViewJoinRumLix", "kafka");

  @Override public void init(StreamGraph graph, Config config) {
    MessageStream<JsonMessage<PageViewEvent>> inputStream1 = graph.createInStream(inputSpec1, null, null, null);
    MessageStream<JsonMessage<PageViewEvent>> inputStream2 = graph.createInStream(inputSpec2, null, null, null);

    MessageStream<JsonMessage<PageViewEvent>> outStream =
        graph.createOutStream(outputSpec, null, null, new StringSerde("UTF-8"), new JsonSerde<>());

    inputStream1
        .join(inputStream2, new MyJoinFunction(), Duration.ofMinutes(1))
        .sendTo(outStream);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    // for remote execution: ApplicationRunner runner = ApplicationRunner.getRemoteRunner(config);
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new NoContextStreamExample());
  }

  class MyJoinFunction implements JoinFunction<String, JsonMessage<PageViewEvent>, JsonMessage<PageViewEvent>, JsonMessage<PageViewEvent>> {
    @Override
    public JsonMessage<PageViewEvent> apply(JsonMessage<PageViewEvent> m1, JsonMessage<PageViewEvent> m2) {
      PageViewEvent newJoinMsg = new PageViewEvent();
      newJoinMsg.joinKey = m1.getKey();
      newJoinMsg.joinFields.addAll(m1.getData().joinFields);
      newJoinMsg.joinFields.addAll(m2.getData().joinFields);
      return new JsonMessage<PageViewEvent>(m1.getData().joinKey, newJoinMsg);
    }

    @Override
    public String getFirstKey(JsonMessage<PageViewEvent> message) {
      return message.getKey();
    }

    @Override
    public String getSecondKey(JsonMessage<PageViewEvent> message) {
      return message.getKey();
    }
  }

  class PageViewEvent {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

}
