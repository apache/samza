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
package org.apache.samza.operators.spec;

import org.apache.samza.operators.KV;
import org.apache.samza.serializers.Serde;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.system.StreamSpec;

/**
 * The spec for an operator that receives incoming messages from an input stream
 * and converts them to the input message.
 *
 * @param <K> the type of input key
 * @param <V> the type of input value
 */
public class InputOperatorSpec<K, V> extends OperatorSpec<KV<K, V>, Object> { // Object == KV<K, V> | V

  private final StreamSpec streamSpec;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final boolean isKeyedInput;

  public InputOperatorSpec(StreamSpec streamSpec,
      Serde<K> keySerde, Serde<V> valueSerde, boolean isKeyedInput, int opId) {
    super(OpCode.INPUT, opId);
    this.streamSpec = streamSpec;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.isKeyedInput = isKeyedInput;
  }

  public StreamSpec getStreamSpec() {
    return this.streamSpec;
  }

  public Serde<K> getKeySerde() {
    return keySerde;
  }

  public Serde<V> getValueSerde() {
    return valueSerde;
  }

  public boolean isKeyedInput() {
    return isKeyedInput;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }  
}
