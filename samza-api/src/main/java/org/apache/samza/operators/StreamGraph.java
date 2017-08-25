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

import java.io.IOException;
import org.apache.samza.annotation.InterfaceStability;

import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.OperatorBiFunction;


/**
 * Provides access to {@link MessageStream}s and {@link OutputStream}s used to describe the processing logic.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of the input key
   * @param <V> the type of the input value
   * @param <M> the type of message in the input {@link MessageStream}
   * @param inputDescriptor the input stream descriptor
   * @param msgBuilder the function to construct the input message from the key-value pair
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> MessageStream<M> getInputStream(StreamDescriptor.Input<K, V> inputDescriptor, OperatorBiFunction<? super K, ? super V, ? extends M> msgBuilder)
      throws IOException;

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of the input key
   * @param <V> the type of the input value
   * @param <M> the type of message in the {@link OutputStream}
   * @param outputDescriptor the output stream descriptor
   * @param keyExtractor the function to extract the key from the message
   * @param msgExtractor the function to extract the value from the message
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  <K, V, M> OutputStream<K, V, M> getOutputStream(StreamDescriptor.Output<K, V> outputDescriptor, MapFunction<? super M, ? extends K> keyExtractor, MapFunction<? super M, ? extends V> msgExtractor);

  /**
   * Sets the {@link ContextManager} for this {@link StreamGraph}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamGraph}
   */
  void setContextManager(ContextManager contextManager);

  /**
   * Sets the dafault {@link IOSystem} for intermediate streams
   *
   * @param defaultSystem default system to automatically create all intermediate streams
   */
  void setDefaultSystem(IOSystem defaultSystem);
}
