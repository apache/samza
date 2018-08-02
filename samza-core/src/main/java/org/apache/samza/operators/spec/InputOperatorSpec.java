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

import java.util.Optional;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * The spec for an operator that receives incoming messages from an input stream
 * and converts them to the input message. The input message type is:
 * <ul>
 *   <li>{@code T} if the input stream has an {@link InputTransformer} with result type T
 *   <li>{@code KV<K, V>} if the input stream is keyed
 *   <li>{@code V} if the input stream is unkeyed
 * </ul>
 */
public class InputOperatorSpec extends OperatorSpec<IncomingMessageEnvelope, Object> {

  private final String streamId;
  private final boolean isKeyed;
  private final InputTransformer<?> transformer; // may be null

  /**
   * The following {@link Serde}s are serialized by the ExecutionPlanner when generating the configs for a stream, and
   * deserialized once during startup in SamzaContainer. They don't need to be deserialized here on a per-task basis
   *
   * Serdes are optional for intermediate streams and may be specified for job.default.system in configuration instead.
   */
  private transient final Optional<Serde> keySerdeOptional;
  private transient final Optional<Serde> valueSerdeOptional;

  public InputOperatorSpec(String streamId, Serde keySerde, Serde valueSerde,
      InputTransformer<?> transformer, boolean isKeyed, String opId) {
    super(OpCode.INPUT, opId);
    this.streamId = streamId;
    this.isKeyed = isKeyed;
    this.transformer = transformer;
    this.keySerdeOptional = Optional.ofNullable(keySerde);
    this.valueSerdeOptional = Optional.ofNullable(valueSerde);
  }

  public String getStreamId() {
    return this.streamId;
  }

  public Optional<Serde> getKeySerde() {
    return keySerdeOptional;
  }

  public Optional<Serde> getValueSerde() {
    return valueSerdeOptional;
  }

  public boolean isKeyed() {
    return isKeyed;
  }

  public Optional<InputTransformer<?>> getTransformer() {
    return Optional.ofNullable(transformer);
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
