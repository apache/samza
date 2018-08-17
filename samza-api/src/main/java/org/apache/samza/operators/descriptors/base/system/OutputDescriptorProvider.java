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
package org.apache.samza.operators.descriptors.base.system;

import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.serializers.Serde;


/**
 * Interface for simple {@code SystemDescriptors} that return {@code OutputDescriptors} parameterized by the type of
 * the provided stream level serde.
 */
public interface OutputDescriptorProvider {

  /**
   * Gets an {@link OutputDescriptor} representing an output stream on this system that uses the provided
   * stream specific serde instead of the default system serde.
   * <p>
   * An {@code OutputStream<KV<K, V>>}, obtained using a descriptor with a {@code KVSerde<K, V>}, can send messages
   * of type {@code KV<K, V>}. An {@code OutputStream<M>} with any other {@code Serde<M>} can send messages of
   * type M without a key.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used if the {@code SystemProducer}
   * serializes the outgoing messages itself, and no prior serialization is required from the framework.
   *
   * @param streamId id of the output stream
   * @param serde serde for this output stream that overrides the default system serde, if any.
   * @param <StreamMessageType> type of messages in the output stream
   * @return the {@link OutputDescriptor} for the output stream
   */
  <StreamMessageType> OutputDescriptor<StreamMessageType, ? extends OutputDescriptor> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde);
}
