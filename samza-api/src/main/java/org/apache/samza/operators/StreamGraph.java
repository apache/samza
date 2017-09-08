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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides access to {@link MessageStream}s and {@link OutputStream}s used to describe the processing logic.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Sets the default {@link Serde} to use for keys.
   * If no default key serde is provided, a no-op serde is used as default.
   *
   * @param keySerde the default key {@link Serde} to use
   */
  void setDefaultKeySerde(Serde<?> keySerde);

  /**
   * Sets the default {@link Serde} to use for messages.
   *
   * @param msgSerde the default message {@link Serde} to use
   */
  void setDefaultMsgSerde(Serde<?> msgSerde);

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param keySerde the {@link Serde} to use for deserializing incoming key
   * @param valueSerde the {@link Serde} to use for deserializing incoming message
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the input {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> MessageStream<M> getInputStream(String streamId, Serde<K> keySerde, Serde<V> valueSerde,
      BiFunction<? super K, ? super V, ? extends M> msgBuilder);

  /**
   * Same as {@link #getInputStream(String, Serde, Serde, BiFunction)}, but uses the default key and message Serdes
   * provided via {@link #setDefaultKeySerde} and {@link #setDefaultMsgSerde(Serde)} to serde types K and V. If no
   * default key and message serdes have been provided <b>before</b> calling this method, a no-op serde is used.
   *
   * @param streamId the unique ID for the stream
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the input {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<? super K, ? super V, ? extends M> msgBuilder);

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param keySerde the {@link Serde} to use for serializing outgoing key from the output message
   * @param msgSerde the {@link Serde} to use for serializing outgoing message from the output message
   * @param keyExtractor the {@link Function} to extract the outgoing key from the output message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the output message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId, Serde<K> keySerde, Serde<V> msgSerde,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor);

  /**
   * Same as {@link #getOutputStream(String, Serde, Serde, Function, Function)}, but uses the default key and
   * message Serdes provided via {@link #setDefaultKeySerde} and {@link #setDefaultMsgSerde(Serde)} to serde
   * types K and V. If no default key and message serdes have been provided <b>before</b> calling this method,
   * a no-op serde is used.
   *
   * @param streamId the unique ID for the stream
   * @param keyExtractor the {@link Function} to extract the outgoing key from the output message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the output message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor);

  /**
   * Sets the {@link ContextManager} for this {@link StreamGraph}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamGraph}
   * @return the {@link StreamGraph} with {@code contextManager} set as its {@link ContextManager}
   */
  StreamGraph withContextManager(ContextManager contextManager);

}
