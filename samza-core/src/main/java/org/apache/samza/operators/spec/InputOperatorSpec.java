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

import java.io.IOException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.TimerFunction;
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

  private final boolean isKeyed;

  // Avoid making per-task copies of StreamSpec/Serde, since the following transient members are only
  // used to generate configuration
  private transient final StreamSpec streamSpec;
  private transient final Serde<K> keySerde;
  private transient final Serde<V> valueSerde;

  public InputOperatorSpec(StreamSpec streamSpec,
      Serde<K> keySerde, Serde<V> valueSerde, boolean isKeyed, String opId) {
    super(OpCode.INPUT, opId);
    this.streamSpec = streamSpec;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.isKeyed = isKeyed;
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

  public boolean isKeyed() {
    return isKeyed;
  }

  public InputOperatorSpec<K, V> copy() throws IOException, ClassNotFoundException {
    return (InputOperatorSpec<K, V>) super.copy();
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return null;
  }
}
