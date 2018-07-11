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

import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;


/**
 * Provides access to {@link MessageStream}s and {@link OutputStream}s used to describe application logic.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Sets the default {@link Serde} to use for (de)serializing messages.
   * <p>.
   * If the default serde is set, it must be set <b>before</b> creating any input or output streams.
   * <p>
   * If no explicit or default serdes are provided, a {@code KVSerde<NoOpSerde, NoOpSerde>} is used. This means that
   * any streams created without explicit or default serdes should be cast to {@code MessageStream<KV<Object, Object>>}.
   * <p>
   * Providing an incompatible message type for the input/output streams that use the default serde will result in
   * {@link ClassCastException}s at runtime.
   *
   * @param serde the default message {@link Serde} to use
   */
  void setDefaultSerde(Serde<?> serde);

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * An input {@code MessageStream<KV<K, V>}, which can be obtained by calling this method with a {@code KVSerde<K, V>},
   * can receive messages of type {@code KV<K, V>}. An input {@code MessageStream<M>} with any other {@code Serde<M>}
   * can receive messages of type M - the key in the incoming message is ignored.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used if the {@code SystemConsumer}
   * deserializes the incoming messages itself, and no further deserialization is required from the framework.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param serde the {@link Serde} to use for deserializing incoming messages
   * @param <M> the type of messages in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde);

  /**
   * Same as {@link #getInputStream(String, Serde)}, but uses the default {@link Serde} provided via
   * {@link #setDefaultSerde(Serde)} for deserializing input messages.
   * <p>
   * If no default serde has been provided <b>before</b> calling this method, a {@code KVSerde<NoOpSerde, NoOpSerde>}
   * is used. Providing a message type {@code M} that is incompatible with the default Serde will result in
   * {@link ClassCastException}s at runtime.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <M> MessageStream<M> getInputStream(String streamId);

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * An {@code OutputStream<KV<K, V>>}, which can be obtained by calling this method with a {@code KVSerde<K, V>},
   * can send messages of type {@code KV<K, V>}. An {@code OutputStream<M>} with any other {@code Serde<M>} can
   * send messages of type M without a key.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used if the {@code SystemProducer}
   * serializes the outgoing messages itself, and no prior serialization is required from the framework.
   * <p>
   * When sending messages to an {@code OutputStream<KV<K, V>>}, messages are partitioned using their serialized key.
   * When sending messages to any other {@code OutputStream<M>}, messages are partitioned using a null partition key.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param serde the {@link Serde} to use for serializing outgoing messages
   * @param <M> the type of messages in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde);

  /**
   * Same as {@link #getOutputStream(String, Serde)}, but uses the default {@link Serde} provided via
   * {@link #setDefaultSerde(Serde)} for serializing output messages.
   * <p>
   * If no default serde has been provided <b>before</b> calling this method, a {@code KVSerde<NoOpSerde, NoOpSerde>}
   * is used. Providing a message type {@code M} that is incompatible with the default Serde will result in
   * {@link ClassCastException}s at runtime.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param <M> the type of messages in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <M> OutputStream<M> getOutputStream(String streamId);

  /**
   * Gets the {@link Table} corresponding to the {@link TableDescriptor}.
   * <p>
   * Multiple invocations of this method with the same {@link TableDescriptor} will throw an
   * {@link IllegalStateException}.
   *
   * @param tableDesc the {@link TableDescriptor}
   * @param <K> the type of the key
   * @param <V> the type of the value
   * @return the {@link Table} corresponding to the {@code tableDesc}
   * @throws IllegalStateException when invoked multiple times with the same {@link TableDescriptor}
   */
  <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc);

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

  static StreamGraph createInstance() {
    try {
      return (StreamGraph) Class.forName("org.apache.samza.operators.StreamGraphSpec").newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new SamzaException("Cannot instantiate an empty StreamGraph to start user application.", e);
    }
  }
}
