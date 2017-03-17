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
package org.apache.samza.operators.stream;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

import java.util.function.BiFunction;

public class InputStreamImpl<K, V, M> extends MessageStreamImpl<M> implements InputStream<K, V, M> {

  private final StreamSpec streamSpec;
  private final Serde<K> keySerde;
  private final Serde<V> msgSerde;
  private final BiFunction<K, V, M> msgBuilder;

  public InputStreamImpl(StreamGraphImpl graph, StreamSpec streamSpec,
      BiFunction<K, V, M> msgBuilder, Serde<K> keySerde, Serde<V> msgSerde) {
    super(graph);
    this.streamSpec = streamSpec;
    this.msgBuilder = msgBuilder;
    this.keySerde = keySerde;
    this.msgSerde = msgSerde;
  }

  public StreamSpec getStreamSpec() {
    return this.streamSpec;
  }

  public Serde<K> getKeySerde() {
    return keySerde;
  }

  public Serde<V> getMsgSerde() {
    return msgSerde;
  }

  public BiFunction<K, V, M> getMsgBuilder() {
    return msgBuilder;
  }
}
