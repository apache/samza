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

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.data.InputMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Example implementation of unique key-based stream-stream join tasks
 *
 */
public class TestJoinExample  extends TestExampleBase {

  TestJoinExample(Set<SystemStreamPartition> inputs) {
    super(inputs);
  }

  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  MessageStream<JsonMessageEnvelope> joinOutput = null;

  @Override
  public void init(StreamGraph graph, Config config) {

    for (SystemStream input : inputs.keySet()) {
      StreamSpec inputStreamSpec = new StreamSpec(input.getSystem() + "-" + input.getStream(), input.getStream(), input.getSystem());
      MessageStream<JsonMessageEnvelope> newSource = graph.<Object, Object, InputMessageEnvelope>createInStream(
          inputStreamSpec, null, null).map(this::getInputMessage);
      if (joinOutput == null) {
        joinOutput = newSource;
      } else {
        joinOutput = joinOutput.join(newSource, new MyJoinFunction(), 1000 * 60);
      }
    }

    joinOutput.sendTo(graph.createOutStream(
            new StreamSpec("joinOutput", "JoinOutputEvent", "kafka"),
            new StringSerde("UTF-8"), new JsonSerde<>()));

  }

  private JsonMessageEnvelope getInputMessage(InputMessageEnvelope ism) {
    return new JsonMessageEnvelope(
        ((MessageType) ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getSystemStreamPartition());
  }

  class MyJoinFunction implements JoinFunction<String, JsonMessageEnvelope, JsonMessageEnvelope, JsonMessageEnvelope> {
    JsonMessageEnvelope myJoinResult(JsonMessageEnvelope m1, JsonMessageEnvelope m2) {
      MessageType newJoinMsg = new MessageType();
      newJoinMsg.joinKey = m1.getKey();
      newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
      newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
      return new JsonMessageEnvelope(m1.getMessage().joinKey, newJoinMsg, null, null);
    }

    @Override
    public JsonMessageEnvelope apply(JsonMessageEnvelope message, JsonMessageEnvelope otherMessage) {
      return this.myJoinResult(message, otherMessage);
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
}
