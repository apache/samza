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
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.Set;
import java.util.function.Supplier;


/**
 * Example implementation of split stream tasks
 *
 */
public class TestBroadcastExample extends TestExampleBase {

  TestBroadcastExample(Set<SystemStreamPartition> inputs) {
    super(inputs);
  }

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
  public void init(StreamGraph graph, Config config) {
    BiFunction<JsonMessageEnvelope, Integer, Integer> sumAggregator = (m, c) -> c + 1;
    Supplier<Integer> initialValue = () -> 0;

    inputs.keySet().forEach(entry -> {
        MessageStream<JsonMessageEnvelope> inputStream = graph.<Object, Object, InputMessageEnvelope>createInStream(
                new StreamSpec(entry.toString(), entry.getStream(), entry.getSystem()), null, null).map(this::getInputMessage);
        inputStream.filter(this::myFilter1).window(Windows.tumblingWindow(Duration.ofMillis(100), initialValue, sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

        inputStream.filter(this::myFilter2).window(Windows.tumblingWindow(Duration.ofMillis(100), initialValue, sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

        inputStream.filter(this::myFilter3).window(Windows.tumblingWindow(Duration.ofMillis(100), initialValue, sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

      });
  }

  JsonMessageEnvelope getInputMessage(InputMessageEnvelope m1) {
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
