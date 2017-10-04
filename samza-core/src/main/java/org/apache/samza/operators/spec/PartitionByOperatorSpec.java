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
import org.apache.samza.operators.functions.WatermarkFunction;

import java.util.function.Function;


/**
 * The spec for an operator that re-partitions a {@link org.apache.samza.operators.MessageStream} to a
 * {@link org.apache.samza.system.SystemStream}. This is usually paired with a corresponding
 * {@link InputOperatorSpec} that consumes the {@link org.apache.samza.system.SystemStream} again.
 * <p>
 * This is a terminal operator and does not allow further operator chaining.
 *
 * @param <M> the type of message
 * @param <K> the type of key in the message
 * @param <V> the type of value in the message
 */
public class PartitionByOperatorSpec<M, K, V> extends OperatorSpec<M, Void> {

  private final OutputStreamImpl<KV<K, V>> outputStream;
  private final Function<? super M, ? extends K> keyFunction;
  private final Function<? super M, ? extends V> valueFunction;

  /**
   * Constructs an {@link PartitionByOperatorSpec} to send messages to the provided {@code outputStream}
   *
   * @param outputStream the {@link OutputStreamImpl} to send messages to
   * @param keyFunction the {@link Function} for extracting the key from the message
   * @param valueFunction the {@link Function} for extracting the value from the message
   * @param opId the unique ID of this {@link SinkOperatorSpec} in the graph
   */
  PartitionByOperatorSpec(OutputStreamImpl<KV<K, V>> outputStream,
      Function<? super M, ? extends K> keyFunction,
      Function<? super M, ? extends V> valueFunction, int opId) {
    super(OpCode.PARTITION_BY, opId);
    this.outputStream = outputStream;
    this.keyFunction = keyFunction;
    this.valueFunction = valueFunction;
  }

  /**
   * The {@link OutputStreamImpl} that this operator is sending its output to.
   * @return the {@link OutputStreamImpl} for this operator if any, else null.
   */
  public OutputStreamImpl<KV<K, V>> getOutputStream() {
    return this.outputStream;
  }

  public Function<? super M, ? extends K> getKeyFunction() {
    return keyFunction;
  }

  public Function<? super M, ? extends V> getValueFunction() {
    return valueFunction;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }
}
