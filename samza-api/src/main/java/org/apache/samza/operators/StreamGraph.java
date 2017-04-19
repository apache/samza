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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides APIs for accessing {@link MessageStream}s to be used to create the DAG of transforms.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Gets the input {@link MessageStream} corresponding to the logical {@code streamId}.
   *
   * @param streamId the unique logical ID for the stream
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the input {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   */
  <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<? super K, ? super V, ? extends M> msgBuilder);

  /**
   * Gets the {@link OutputStream} corresponding to the logical {@code streamId}.
   *
   * @param streamId the unique logical ID for the stream
   * @param keyExtractor the {@link Function} to extract the outgoing key from the output message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the output message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStream}
   * @return the output {@link MessageStream}
   */
  <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor);

  /**
   * Sets the {@link ContextManager} for this {@link StreamGraph}.
   *
   * The provided {@code contextManager} will be initialized before the transformation functions
   * and can be used to setup shared context between them.
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamGraph}
   * @return the {@link StreamGraph} with the {@code contextManager} as its {@link ContextManager}
   */
  StreamGraph withContextManager(ContextManager contextManager);

}
