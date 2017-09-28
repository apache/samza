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
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.function.Supplier;


/**
 * Example implementation of a simple user-defined task w/ a window operator.
 *
 */
public class WindowExample implements StreamApplication {

  @Override
  public void init(StreamGraph graph, Config config) {
    Supplier<Integer> initialValue = () -> 0;
    FoldLeftFunction<PageViewEvent, Integer> counter = (m, c) -> c == null ? 1 : c + 1;
    MessageStream<PageViewEvent> inputStream = graph.getInputStream("inputStream", new JsonSerdeV2<PageViewEvent>());
    OutputStream<Integer> outputStream = graph.getOutputStream("outputStream", new IntegerSerde());

    // create a tumbling window that outputs the number of message collected every 10 minutes.
    // also emit early results if either the number of messages collected reaches 30000, or if no new messages arrive
    // for 1 minute.
    inputStream
        .window(Windows.tumblingWindow(Duration.ofMinutes(10), initialValue, counter)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceLastMessage(Duration.ofMinutes(1)))))
        .map(WindowPane::getMessage)
        .sendTo(outputStream);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    LocalApplicationRunner localRunner = new LocalApplicationRunner(config);
    localRunner.run(new WindowExample());
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
