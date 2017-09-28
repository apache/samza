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
package org.apache.samza.operators.impl.store;

import org.apache.samza.serializers.Serde;

import java.nio.ByteBuffer;

public class TimestampedValueSerde<V> implements Serde<TimestampedValue<V>> {
  private final Serde<V> vSerde;

  public TimestampedValueSerde(Serde<V> vSerde) {
    this.vSerde = vSerde;
  }

  @Override
  public TimestampedValue<V> fromBytes(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    byte[] vBytes = new byte[bytes.length - 8 /* long ts bytes */];
    bb.get(vBytes, 0, vBytes.length);
    V v = vSerde.fromBytes(vBytes);
    long ts = bb.getLong();
    return new TimestampedValue<>(v, ts);
  }

  @Override
  public byte[] toBytes(TimestampedValue<V> tv) {
    byte[] vBytes = vSerde.toBytes(tv.getValue());
    ByteBuffer bb = ByteBuffer.allocate(vBytes.length + 8 /* long ts bytes */);
    bb.put(vBytes);
    bb.putLong(tv.getTimestamp());
    return bb.array();
  }
}
