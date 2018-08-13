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

import com.google.common.collect.ImmutableList;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamAppDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;

public class MergeExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new MergeExample(), config);

    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(StreamAppDescriptor appDesc) {
    KVSerde<String, PageViewEvent>
        pgeMsgSerde = KVSerde.of(new StringSerde("UTF-8"), new JsonSerdeV2<>(PageViewEvent.class));

    MessageStream.mergeAll(ImmutableList.of(appDesc.getInputStream("viewStream1", pgeMsgSerde),
        appDesc.getInputStream("viewStream2", pgeMsgSerde), appDesc.getInputStream("viewStream3", pgeMsgSerde)))
        .sendTo(appDesc.getOutputStream("mergedStream", pgeMsgSerde));
  }

  class PageViewEvent {
    String pageId;
    long viewTimestamp;
  }
}