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
import org.apache.samza.application.internal.StreamApplicationBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.ApplicationRuntimes;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;


/**
 * Example implementation of a task that splits its input into multiple output streams.
 */
public class BroadcastExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRuntime app = ApplicationRuntimes.createStreamApp(new BroadcastExample(), config);
    app.start();
    app.waitForFinish();
  }

  @Override
  public void init(StreamApplicationBuilder appBuilder, Config config) {
    KVSerde<String, PageViewEvent> pgeMsgSerde = KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageViewEvent.class));
    MessageStream<KV<String, PageViewEvent>> inputStream = appBuilder.getInputStream("pageViewEventStream", pgeMsgSerde);

    inputStream.filter(m -> m.key.equals("key1")).sendTo(appBuilder.getOutputStream("outStream1", pgeMsgSerde));
    inputStream.filter(m -> m.key.equals("key2")).sendTo(appBuilder.getOutputStream("outStream2", pgeMsgSerde));
    inputStream.filter(m -> m.key.equals("key3")).sendTo(appBuilder.getOutputStream("outStream3", pgeMsgSerde));

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
