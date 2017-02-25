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
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.data.InputMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.Set;


/**
 * Example implementation of a simple user-defined tasks w/ window operators
 *
 */
public class TestWindowExample extends TestExampleBase {
  class MessageType {
    String field1;
    String field2;
  }

  TestWindowExample(Set<SystemStreamPartition> inputs) {
    super(inputs);
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {

    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  @Override
  public void init(StreamGraph graph, Config config) {
    BiFunction<JsonMessageEnvelope, Integer, Integer> maxAggregator = (m, c) -> c + 1;
    inputs.keySet().forEach(source -> graph.<Object, Object, InputMessageEnvelope>createInStream(
            new StreamSpec(source.toString(), source.getStream(), source.getSystem()), null, null).
        map(m1 -> new JsonMessageEnvelope(this.myMessageKeyFunction(m1), (MessageType) m1.getMessage(), m1.getOffset(),
            m1.getSystemStreamPartition())).window(Windows.tumblingWindow(Duration.ofMillis(200), maxAggregator)));

  }

  String myMessageKeyFunction(MessageEnvelope<Object, Object> m) {
    return m.getKey().toString();
  }

}
