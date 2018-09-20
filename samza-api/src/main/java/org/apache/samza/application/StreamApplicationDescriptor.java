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
package org.apache.samza.application;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.table.Table;


/**
 * The interface class to describe a {@link SamzaApplication} in high-level API in Samza.
 */
@InterfaceStability.Evolving
public interface StreamApplicationDescriptor extends ApplicationDescriptor<StreamApplicationDescriptor> {

  /**
   * Sets the default SystemDescriptor to use for intermediate streams. This is equivalent to setting
   * {@code job.default.system} and its properties in configuration.
   * <p>
   * If the default system descriptor is set, it must be set <b>before</b> creating any input/output/intermediate streams.
   * <p>
   * If an input/output stream is created with a stream-level Serde, they will be used, else the serde specified
   * for the {@code job.default.system} in configuration will be used.
   * <p>
   * Providing an incompatible message type for the intermediate streams that use the default serde will result in
   * {@link ClassCastException}s at runtime.
   *
   * @param defaultSystemDescriptor the default system descriptor to use
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code defaultSystemDescriptor} set as its default system
   */
  StreamApplicationDescriptor withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor);

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code inputDescriptor}.
   * <p>
   * A {@code MessageStream<KV<K, V>}, obtained by calling this method with a descriptor with a {@code KVSerde<K, V>},
   * can receive messages of type {@code KV<K, V>}. An input {@code MessageStream<M>}, obtained using a descriptor with
   * any other {@code Serde<M>}, can receive messages of type M - the key in the incoming message is ignored.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used for the descriptor if the
   * {@code SystemConsumer} deserializes the incoming messages itself, and no further deserialization is required from
   * the framework.
   * <p>
   * Multiple invocations of this method with the same {@code inputDescriptor} will throw an
   * {@link IllegalStateException}.
   *
   * @param inputDescriptor the descriptor for the stream
   * @param <M> the type of messages in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code inputDescriptor}
   */
  <M> MessageStream<M> getInputStream(InputDescriptor<M, ?> inputDescriptor);

  /**
   * Gets the {@link OutputStream} corresponding to the {@code outputDescriptor}.
   * <p>
   * An {@code OutputStream<KV<K, V>>}, obtained by calling this method with a descriptor with a {@code KVSerde<K, V>},
   * can send messages of type {@code KV<K, V>}. An {@code OutputStream<M>}, obtained using a descriptor with any
   * other {@code Serde<M>}, can send messages of type M without a key.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used for the descriptor if the
   * {@code SystemProducer} serializes the outgoing messages itself, and no prior serialization is required from
   * the framework.
   * <p>
   * When sending messages to an {@code OutputStream<KV<K, V>>}, messages are partitioned using their serialized key.
   * When sending messages to any other {@code OutputStream<M>}, messages are partitioned using a null partition key.
   * <p>
   * Multiple invocations of this method with the same {@code outputDescriptor} will throw an
   * {@link IllegalStateException}.
   *
   * @param outputDescriptor the descriptor for the stream
   * @param <M> the type of messages in the {@link OutputStream}
   * @return the {@link OutputStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code outputDescriptor}
   */
  <M> OutputStream<M> getOutputStream(OutputDescriptor<M, ?> outputDescriptor);

  /**
   * Gets the {@link Table} corresponding to the {@link TableDescriptor}.
   * <p>
   * Multiple invocations of this method with the same {@link TableDescriptor} will throw an
   * {@link IllegalStateException}.
   *
   * @param tableDescriptor the {@link TableDescriptor}
   * @param <K> the type of the key
   * @param <V> the type of the value
   * @return the {@link Table} corresponding to the {@code tableDescriptor}
   * @throws IllegalStateException when invoked multiple times with the same {@link TableDescriptor}
   */
  <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDescriptor);
}