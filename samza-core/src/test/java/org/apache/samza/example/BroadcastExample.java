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
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.util.CommandLine;


/**
 * Example implementation of a task that splits its input into multiple output streams.
 */
public class BroadcastExample implements StreamApplication {

  private final StreamSpec inputStreamSpec = new StreamSpec("inputStream", "inputStream", "inputSystem");
  private final StreamSpec outputStreamSpec1 = new StreamSpec("outputStream1", "outputStream1", "outputSystem");
  private final StreamSpec outputStreamSpec2 = new StreamSpec("outputStream2", "outputStream2", "outputSystem");
  private final StreamSpec outputStreamSpec3 = new StreamSpec("outputStream3", "outputStream3", "outputSystem");

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<JsonMessage<PageViewEvent>> inputStream = graph.createInStream(inputStreamSpec, (k, m) -> m,
        new StringSerde("UTF-8"), new JsonSerde<JsonMessage<PageViewEvent>>());

    MessageStream<JsonMessage<PageViewEvent>> outputStream1 =
        graph.createOutStream(outputStreamSpec1, m -> m.getData().key, m -> m,
            new StringSerde("UTF-8"), new JsonSerde<JsonMessage<PageViewEvent>>());
    MessageStream<JsonMessage<PageViewEvent>> outputStream2 =
        graph.createOutStream(outputStreamSpec2, m -> m.getData().key, m -> m,
            new StringSerde("UTF-8"), new JsonSerde<JsonMessage<PageViewEvent>>());
    MessageStream<JsonMessage<PageViewEvent>> outputStream3 =
        graph.createOutStream(outputStreamSpec3, m -> m.getData().key, m -> m,
            new StringSerde("UTF-8"), new JsonSerde<JsonMessage<PageViewEvent>>());

    inputStream.filter(m -> m.getData().key.equals("key1")).sendTo(outputStream1);
    inputStream.filter(m -> m.getData().key.equals("key2")).sendTo(outputStream2);
    inputStream.filter(m -> m.getData().key.equals("key3")).sendTo(outputStream3);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    // for remote execution: ApplicationRunner runner = ApplicationRunner.getRemoteRunner(config);
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new BroadcastExample());
  }

  class PageViewEvent {
    String key;
    long timestamp;

    public PageViewEvent(String key, long timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }
  }

}
