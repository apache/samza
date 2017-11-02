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

package org.apache.samza.serializers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A serializer for UUID
 */
public class UUIDSerde implements Serde<UUID> {

  public byte[] toBytes(UUID obj) {
    if (obj != null) {
      return ByteBuffer.allocate(16)
          .putLong(obj.getMostSignificantBits())
          .putLong(obj.getLeastSignificantBits())
          .array();
    } else {
      return null;
    }
  }

  public UUID fromBytes(byte[] bytes) {
    if (bytes != null) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return new UUID(buffer.getLong(), buffer.getLong());
    } else {
      return null;
    }
  }
}
