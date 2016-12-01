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

package org.apache.samza.operators;

import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.data.JsonInputSystemMessage;
import org.apache.samza.system.SystemStreamPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Example implementation of unique key-based stream-stream join tasks
 *
 */
public class JoinTask implements StreamOperatorTask {
  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessage extends JsonInputSystemMessage<MessageType> {
    JsonMessage(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  MessageStream<JsonMessage> joinOutput = null;

  @Override
  public void transform(Map<SystemStreamPartition, MessageStream<IncomingSystemMessage>> messageStreams) {
    messageStreams.values().forEach(messageStream -> {
        MessageStream<JsonMessage> newSource = messageStream.map(this::getInputMessage);
        if (joinOutput == null) {
          joinOutput = newSource;
        } else {
          joinOutput = joinOutput.join(newSource, (m1, m2) -> this.myJoinResult(m1, m2));
        }
      });
  }

  private JsonMessage getInputMessage(IncomingSystemMessage ism) {
    return new JsonMessage(
        ((MessageType) ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getSystemStreamPartition());
  }

  JsonMessage myJoinResult(JsonMessage m1, JsonMessage m2) {
    MessageType newJoinMsg = new MessageType();
    newJoinMsg.joinKey = m1.getKey();
    newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
    newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
    return new JsonMessage(m1.getMessage().joinKey, newJoinMsg, null, null);
  }
}
