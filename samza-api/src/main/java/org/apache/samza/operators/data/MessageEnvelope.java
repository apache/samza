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

package org.apache.samza.operators.data;

import org.apache.samza.annotation.InterfaceStability;


/**
 * An entry in the input/output {@link org.apache.samza.operators.MessageStream}s
 */
@InterfaceStability.Unstable
public interface MessageEnvelope<K, M> {

  /**
   * Get the key for this {@link MessageEnvelope}.
   *
   * @return  the key for this {@link MessageEnvelope}
   */
  K getKey();

  /**
   * Get the message in this {@link MessageEnvelope}.
   *
   * @return  the message in this {@link MessageEnvelope}
   */
  M getMessage();

  /**
   * Whether this {@link MessageEnvelope} indicates deletion of a previous message with this key.
   *
   * @return  true if the current {@link MessageEnvelope} indicates deletion of a previous message with this key
   */
  default boolean isDelete() {
    return false;
  }

}
