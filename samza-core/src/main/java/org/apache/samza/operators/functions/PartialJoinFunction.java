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

import org.apache.samza.storage.kv.KeyValueStore;

/**
 * An internal function that maintains state and join logic for one side of a two-way join.
 */
public interface PartialJoinFunction<K, M, JM, RM> extends InitableFunction {

  /**
   * Joins a message in this stream with a message from another stream.
   *
   * @param m  message from this input stream
   * @param jm  message from the other input stream
   * @return  the joined message in the output stream
   */
  RM apply(M m, JM jm);

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
  KeyValueStore<K, PartialJoinMessage<M>> getState();

  class PartialJoinMessage<M> {
    private final M message;
    private final long receivedAt;

    public PartialJoinMessage(M message, long receivedAt) {
      this.message = message;
      this.receivedAt = receivedAt;
    }

    public M getMessage() {
      return message;
    }

    public long getReceivedAt() {
      return receivedAt;
    }
  }
}
