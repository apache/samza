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
 * This class defines the generic interface of {@link Message}, which is a entry in the input/output stream.
 *
 * <p>The {@link Message} models the basic operatible unit in streaming SQL processes in Samza.
 *
 */
@InterfaceStability.Unstable
public interface Message<K, M> {

  /**
   * Access method to get the corresponding message body in {@link Message}
   *
   * @return Message object in this {@link Message}
   */
  M getMessage();

  /**
   * Method to indicate whether this {@link Message} indicates deletion of a message w/ the message key
   *
   * @return A boolean value indicates whether the current message is a delete or insert message
   */
  default boolean isDelete() {
    return false;
  };

  /**
   * Access method to the key of the message
   *
   * @return The key of the message
   */
  K getKey();

  /**
   * Get the message creation timestamp of the message.
   *
   * @return The message's timestamp in nano seconds.
   */
  long getTimestamp();

}
