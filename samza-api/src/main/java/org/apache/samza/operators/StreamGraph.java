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

/**
 * Allows creating input and output {@link MessageStream}s to be used in the application.
 */
@InterfaceStability.Unstable
public interface StreamGraph {

  /**
   * Add an input {@link MessageStream} to the graph.
   *
   * @param streamId the unique ID for the stream
   * @param msgBuilder the function to convert the incoming key and message to a message in the input {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   */
  <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<K, V, M> msgBuilder);

  /**
   * Set the {@link ContextManager} for the {@link StreamGraph}.
   *
   * @param manager the {@link ContextManager} to use for the {@link StreamGraph}
   * @return the {@link StreamGraph} with the {@code manager} as its {@link ContextManager}
   */
  StreamGraph withContextManager(ContextManager manager);

}
