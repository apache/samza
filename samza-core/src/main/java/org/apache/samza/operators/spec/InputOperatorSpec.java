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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.system.StreamSpec;

import java.util.function.BiFunction;

/**
 * The spec for an operator that receives incoming messages from an input stream
 * and converts them to the input message.
 *
 * @param <K> the type of key in the incoming message
 * @param <V> the type of message in the incoming message
 * @param <M> the type of input message
 */
public class InputOperatorSpec<K, V, M> extends OperatorSpec<Pair<K, V>, M> {

  private final StreamSpec streamSpec;
  private final BiFunction<K, V, M> msgBuilder;

  public InputOperatorSpec(StreamSpec streamSpec, BiFunction<K, V, M> msgBuilder, int opId) {
    super(OpCode.INPUT, opId);
    this.streamSpec = streamSpec;
    this.msgBuilder = msgBuilder;
  }

  public StreamSpec getStreamSpec() {
    return this.streamSpec;
  }

  public BiFunction<K, V, M> getMsgBuilder() {
    return this.msgBuilder;
  }

  @Override
  public InitableFunction getTransformFn() {
    return null;
  }
}
