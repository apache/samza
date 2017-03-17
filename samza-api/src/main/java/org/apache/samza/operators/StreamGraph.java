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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Allows creating input and output {@link MessageStream}s to be used in the application.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Add an input {@link MessageStream} to the graph.
   *
   * @param streamSpec the {@link StreamSpec} describing the physical characteristics of the input {@link MessageStream}
   * @param msgBuilder the function to convert the incoming key and message to a message in the input {@link MessageStream}
   * @param keySerde the serde used to deserialize the incoming message key
   * @param msgSerde the serde used to deserialize the incoming message body
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   */
  <K, V, M> MessageStream<M> createInStream(StreamSpec streamSpec,
      BiFunction<K, V, M> msgBuilder,
      Serde<K> keySerde, Serde<V> msgSerde);

  /**
   * Add an output {@link MessageStream} to the graph.
   *
   * @param streamSpec the {@link StreamSpec} describing the physical characteristics of the output {@link MessageStream}
   * @param keyExtractor the function to extract the outgoing key from a message in the output {@link MessageStream}
   * @param msgExtractor the function to extract the outgoing message from a message in the output {@link MessageStream}
   * @param keySerde the serde used to serialize the outgoing message key
   * @param msgSerde the serde used to serialize the outgoing message body
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the output {@link MessageStream}
   * @return the output {@link MessageStream}
   */
  <K, V, M> MessageStream<M> createOutStream(StreamSpec streamSpec,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor,
      Serde<K> keySerde, Serde<V> msgSerde);

  /**
   * Set the {@link ContextManager} for the {@link StreamGraph}.
   *
   * @param manager the {@link ContextManager} to use for the {@link StreamGraph}
   * @return the {@link StreamGraph} with the {@code manager} as its {@link ContextManager}
   */
  StreamGraph withContextManager(ContextManager manager);

}
