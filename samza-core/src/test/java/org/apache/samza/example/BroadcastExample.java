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
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.util.CommandLine;


/**
 * Example implementation of a task that splits its input into multiple output streams.
 */
public class BroadcastExample implements StreamApplication {

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<PageViewEvent> inputStream = graph.getInputStream("inputStream", (k, m) -> (PageViewEvent) m);

    inputStream.filter(m -> m.key.equals("key1")).sendTo("outputStream1", m -> m.key);
    inputStream.filter(m -> m.key.equals("key2")).sendTo("outputStream2", m -> m.key);
    inputStream.filter(m -> m.key.equals("key3")).sendTo("outputStream3", m -> m.key);
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
