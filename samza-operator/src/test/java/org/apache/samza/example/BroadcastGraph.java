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
import org.apache.samza.operators.StreamGraphFactory;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.InputMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.Properties;
import java.util.Set;


/**
 * Example implementation of split stream tasks
 *
 */
public class BroadcastGraph implements StreamGraphFactory {

  private final Set<SystemStreamPartition> inputs;

  BroadcastGraph(Set<SystemStreamPartition> inputs) {
    this.inputs = inputs;
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
  public StreamGraph create(Config config) {
    StreamGraphImpl graph = new StreamGraphImpl();

    BiFunction<JsonMessageEnvelope, Integer, Integer> sumAggregator = (m, c) -> c + 1;
    inputs.forEach(entry -> {
        MessageStream<JsonMessageEnvelope> inputStream = graph.<Object, Object, InputMessageEnvelope>createInStream(new StreamSpec() {
          @Override public SystemStream getSystemStream() {
            return entry.getSystemStream();
          }

          @Override public Properties getProperties() {
            return null;
          }
        }, null, null).
            map(this::getInputMessage);

        inputStream.filter(this::myFilter1).window(Windows.tumblingWindow(Duration.ofMillis(100), sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

        inputStream.filter(this::myFilter2).window(Windows.tumblingWindow(Duration.ofMillis(100), sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

        inputStream.filter(this::myFilter3).window(Windows.tumblingWindow(Duration.ofMillis(100), sumAggregator)
            .setLateTrigger(Triggers.any(Triggers.count(30000), Triggers.timeSinceFirstMessage(Duration.ofMillis(10)))));

      });
    return graph;
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
