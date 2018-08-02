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
package org.apache.samza.operators.functions;

import org.apache.samza.util.TimestampedValue;
import org.apache.samza.storage.kv.KeyValueStore;

/**
 * An internal function that maintains state and join logic for one side of a two-way join.
 */
public interface PartialJoinFunction<K, M, OM, JM> extends InitableFunction, ClosableFunction {

  /**
   * Joins a message in this stream with a message from another stream.
   *
   * @param m  message from this input stream
   * @param om  message from the other input stream
   * @return  the joined message in the output stream
   */
  JM apply(M m, OM om);

  /**
   * Gets the key for the input message.
   *
   * @param message  the input message from the first stream
   * @return  the join key in the {@code message}
   */
  K getKey(M message);

  /**
   * Gets the state associated with this stream.
   *
   * @return the key value store containing the state for this stream
   */
  KeyValueStore<K, TimestampedValue<M>> getState();

}
