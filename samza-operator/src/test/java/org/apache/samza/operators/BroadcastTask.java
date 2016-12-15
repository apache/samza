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


import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.windows.TriggerBuilder;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Collection;
import java.util.Map;


/**
 * Example implementation of split stream tasks
 *
 */
public class BroadcastTask implements StreamOperatorTask {
  class MessageType {
    String field1;
    String field2;
    String field3;
    String field4;
    String parKey;
    private long timestamp;

    public long getTimestamp() {
      return this.timestamp;
    }
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  @Override
  public void transform(Map<SystemStreamPartition, MessageStream<IncomingSystemMessageEnvelope>> messageStreams) {
    messageStreams.values().forEach(entry -> {
        MessageStream<JsonMessageEnvelope> inputStream = entry.map(this::getInputMessage);

        inputStream.filter(this::myFilter1).
          window(Windows.<JsonMessageEnvelope, String>intoSessionCounter(
              m -> String.format("%s-%s", m.getMessage().field1, m.getMessage().field2)).
            setTriggers(TriggerBuilder.<JsonMessageEnvelope, Integer>earlyTriggerWhenExceedWndLen(100).
              addLateTriggerOnSizeLimit(10).
              addTimeoutSinceLastMessage(30000)));

        inputStream.filter(this::myFilter2).
          window(Windows.<JsonMessageEnvelope, String>intoSessions(
              m -> String.format("%s-%s", m.getMessage().field3, m.getMessage().field4)).
            setTriggers(TriggerBuilder.<JsonMessageEnvelope, Collection<JsonMessageEnvelope>>earlyTriggerWhenExceedWndLen(100).
              addTimeoutSinceLastMessage(30000)));

        inputStream.filter(this::myFilter3).
          window(Windows.<JsonMessageEnvelope, String, MessageType>intoSessions(
              m -> String.format("%s-%s", m.getMessage().field3, m.getMessage().field4), m -> m.getMessage()).
            setTriggers(TriggerBuilder.<JsonMessageEnvelope, Collection<MessageType>>earlyTriggerOnEventTime(m -> m.getMessage().getTimestamp(), 30000).
              addTimeoutSinceFirstMessage(60000)));
      });
  }

  JsonMessageEnvelope getInputMessage(IncomingSystemMessageEnvelope m1) {
    return (JsonMessageEnvelope) m1.getMessage();
  }

  boolean myFilter1(JsonMessageEnvelope m1) {
    // Do user defined processing here
    return m1.getMessage().parKey.equals("key1");
  }

  boolean myFilter2(JsonMessageEnvelope m1) {
    // Do user defined processing here
    return m1.getMessage().parKey.equals("key2");
  }

  boolean myFilter3(JsonMessageEnvelope m1) {
    return m1.getMessage().parKey.equals("key3");
  }
}
