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
 *
 */
package org.apache.samza.operators.impl.store;

import org.apache.samza.serializers.Serde;
import java.nio.ByteBuffer;

/**
 * A {@link Serde} for {@link TimeSeriesKey}s.
 *
 * <p> This wraps the actual key's serde with serializers for timestamp and sequence number.
 *
 *  A {@link TimeSeriesKeySerde} serializes a key as follows:
 *    +-------------------------+------------------+------------+
 *    |  serialized-key bytes   |  timestamp       | seq num    |
 *    |(serialized by keySerde) |                  |            |
 *    +-------------------------+------------------+------------+
 *    +---serialized key len----+-------8 bytes----+---4 bytes--+
 *
 * @param <K>, the type of the wrapped key
 */
public class TimeSeriesKeySerde<K> implements Serde<TimeSeriesKey<K>> {

  private static final int TIMESTAMP_SIZE = 8;
  private static final int SEQNUM_SIZE = 4;

  private final Serde<K> keySerde;

  public TimeSeriesKeySerde(Serde<K> keySerde) {
    this.keySerde = keySerde;
  }

  @Override
  public byte[] toBytes(TimeSeriesKey<K> timeSeriesKey) {
    K key = timeSeriesKey.getKey();
    long timestamp = timeSeriesKey.getTimestamp();
    int seqNum = timeSeriesKey.getSeqNum();

    byte[] serializedKey = keySerde.toBytes(key);
    int keySize = serializedKey == null ? 0 : serializedKey.length;

    // append the timestamp and sequence number to the serialized key bytes
    ByteBuffer buf = ByteBuffer.allocate(keySize + TIMESTAMP_SIZE + SEQNUM_SIZE);
    if (serializedKey != null) {
      buf.put(serializedKey);
    }
    buf.putLong(timestamp);
    buf.putInt(seqNum);

    return buf.array();
  }

  @Override
  public TimeSeriesKey<K> fromBytes(byte[] timeSeriesKeyBytes) {
    // First obtain the key bytes, and deserialize them. Later de-serialize the timestamp and sequence number
    ByteBuffer buf = ByteBuffer.wrap(timeSeriesKeyBytes);
    int keySize =  timeSeriesKeyBytes.length - TIMESTAMP_SIZE - SEQNUM_SIZE;
    K key = null;

    if (keySize != 0) {
      byte[] keyBytes = new byte[keySize];
      buf.get(keyBytes);
      key = keySerde.fromBytes(keyBytes);
    }

    long timeStamp = buf.getLong();
    int seqNum = buf.getInt();

    return new TimeSeriesKey(key, timeStamp, seqNum);
  }
}
