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

package org.apache.samza.system;

/**
 * The type of the intermediate stream message. The enum will be encoded using its ordinal value and
 * put in the first byte of the serialization of intermediate message.
 */
public enum MessageType {
  USER_MESSAGE,
  WATERMARK,
  END_OF_STREAM;

  /**
   * Returns the {@link MessageType} of a particular intermediate stream message.
   * @param message an intermediate stream message
   * @return type of the message
   */
  public static MessageType of(Object message) {
    if (message instanceof WatermarkMessage) {
      return WATERMARK;
    } else if (message instanceof EndOfStreamMessage) {
      return END_OF_STREAM;
    } else {
      return USER_MESSAGE;
    }
  }
}
