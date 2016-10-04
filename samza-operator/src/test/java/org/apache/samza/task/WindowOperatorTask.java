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

import org.apache.samza.operators.api.MessageStreams.SystemMessageStream;
import org.apache.samza.operators.api.TriggerBuilder;
import org.apache.samza.operators.api.Windows;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.operators.api.data.Offset;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Collection;


/**
 * Example implementation of a simple user-defined tasks w/ window operators
 *
 */
public class WindowOperatorTask implements StreamOperatorTask {
  class MessageType {
    String field1;
    String field2;
  }

  class JsonMessage extends InputJsonSystemMessage<MessageType> {

    JsonMessage(String key, MessageType data, Offset offset, long timestamp, SystemStreamPartition partition) {
      super(key, data, offset, timestamp, partition);
    }
  }

  @Override public void initOperators(Collection<SystemMessageStream> sources) {
    sources.forEach(source ->
      source.map(m1 ->
        new JsonMessage(
          this.myMessageKeyFunction(m1),
          (MessageType) m1.getMessage(),
          m1.getOffset(),
          m1.getTimestamp(),
          m1.getSystemStreamPartition())).
        window(
          Windows.<JsonMessage, String>intoSessionCounter(
              m -> String.format("%s-%s", m.getMessage().field1, m.getMessage().field2)).
            setTriggers(TriggerBuilder.<JsonMessage, Integer>earlyTriggerWhenExceedWndLen(100).
              addTimeoutSinceLastMessage(30000)))
    );
  }

  String myMessageKeyFunction(Message<Object, Object> m) {
    return m.getKey().toString();
  }

}
