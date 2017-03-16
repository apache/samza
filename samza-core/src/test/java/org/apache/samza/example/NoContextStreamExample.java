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
import org.apache.samza.operators.data.InputMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * Example {@link StreamGraphBuilder} code to test the API methods
 */
public class NoContextStreamExample implements StreamGraphBuilder {

  StreamSpec input1 = new StreamSpec("inputStreamA", "PageViewEvent", "kafka");

  StreamSpec input2 = new StreamSpec("inputStreamB", "RumLixEvent", "kafka");

  StreamSpec output = new StreamSpec("joinedPageViewStream", "PageViewJoinRumLix", "kafka");

  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  private JsonMessageEnvelope getInputMessage(InputMessageEnvelope ism) {
    return new JsonMessageEnvelope(
        ((MessageType) ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getSystemStreamPartition());
  }

  class MyJoinFunction implements JoinFunction<String, JsonMessageEnvelope, JsonMessageEnvelope, JsonIncomingSystemMessageEnvelope<MessageType>> {

    @Override
    public JsonIncomingSystemMessageEnvelope<MessageType> apply(JsonMessageEnvelope m1,
        JsonMessageEnvelope m2) {
      MessageType newJoinMsg = new MessageType();
      newJoinMsg.joinKey = m1.getKey();
      newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
      newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
      return new JsonMessageEnvelope(m1.getMessage().joinKey, newJoinMsg, null, null);
    }

    @Override
    public String getFirstKey(JsonMessageEnvelope message) {
      return message.getKey();
    }

    @Override
    public String getSecondKey(JsonMessageEnvelope message) {
      return message.getKey();
    }
  }


  /**
   * used by remote application runner to launch the job in remote program. The remote program should follow the similar
   * invoking context as in local:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ApplicationRunner runner = ApplicationRunner.fromConfig(config);
   *     runner.run(new NoContextStreamExample(), config);
   *   }
   *
   */
  @Override public void init(StreamGraph graph, Config config) {
    MessageStream<InputMessageEnvelope> inputSource1 = graph.<Object, Object, InputMessageEnvelope>createInStream(
        input1, null, null);
    MessageStream<InputMessageEnvelope> inputSource2 = graph.<Object, Object, InputMessageEnvelope>createInStream(
        input2, null, null);
    OutputStream<JsonIncomingSystemMessageEnvelope<MessageType>> outStream = graph.createOutStream(output,
        new StringSerde("UTF-8"), new JsonSerde<>());

    inputSource1.map(this::getInputMessage).
        join(inputSource2.map(this::getInputMessage), new MyJoinFunction(), Duration.ofMinutes(1)).
        sendTo(outStream);

  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new NoContextStreamExample());
  }

}
