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

package org.apache.samza.task;

import org.apache.samza.operators.api.MessageStream;
import org.apache.samza.operators.api.MessageStreams.SystemMessageStream;
import org.apache.samza.operators.api.data.IncomingSystemMessage;
import org.apache.samza.operators.api.data.Offset;
import org.apache.samza.system.SystemStreamPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Example implementation of unique key-based stream-stream join tasks
 *
 */
public class JoinOperatorTask implements StreamOperatorTask {
  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessage extends InputJsonSystemMessage<MessageType> {

    JsonMessage(String key, MessageType data, Offset offset, long timestamp, SystemStreamPartition partition) {
      super(key, data, offset, timestamp, partition);
    }
  }

  MessageStream<JsonMessage> joinOutput = null;

  @Override public void initOperators(Collection<SystemMessageStream> sources) {
    sources.forEach(source -> {
      MessageStream<JsonMessage> newSource = source.map(this::getInputMessage);
      if (joinOutput == null) {
        joinOutput = newSource;
      } else {
        joinOutput = joinOutput.join(newSource, (m1, m2) -> this.myJoinResult(m1, m2));
      }
    });
  }

  private JsonMessage getInputMessage(IncomingSystemMessage ism) {
    return new JsonMessage(
        ((MessageType)ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getTimestamp(),
        ism.getSystemStreamPartition());
  }

  JsonMessage myJoinResult(JsonMessage m1, JsonMessage m2) {
    MessageType newJoinMsg = new MessageType();
    newJoinMsg.joinKey = m1.getKey();
    newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
    newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
    return new JsonMessage(m1.getMessage().joinKey, newJoinMsg, null, m1.getTimestamp(), null);
  }
}
