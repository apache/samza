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

import org.apache.samza.operators.KV;

import java.nio.ByteBuffer;


/**
 * A marker serde class to indicate that messages are keyed and should be deserialized as K-V pairs. This class is
 * intended for use cases where a single Serde parameter or configuration is required.
 *
 * @param <K> type of the key in the message
 * @param <V> type of the value in the message
 */
public class KVSerde<K, V> implements Serde<KV<K, V>> {

  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;

  public KVSerde(Serde<K> keySerde, Serde<V> valueSerde) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public static <K, V> KVSerde<K, V> of(Serde<K> keySerde, Serde<V> valueSerde) {
    return new KVSerde<>(keySerde, valueSerde);
  }

  public KV<K, V> fromBytes(byte[] bytes) {
    if (bytes != null) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      int keyLength = byteBuffer.getInt();
      byte[] keyBytes = new byte[keyLength];
      byteBuffer.get(keyBytes);
      int valueLength = byteBuffer.getInt();
      byte[] valueBytes = new byte[valueLength];
      byteBuffer.get(valueBytes);
      K key = keySerde.fromBytes(keyBytes);
      V value = valueSerde.fromBytes(valueBytes);
      return KV.of(key, value);
    } else {
      return null;
    }
  }

  public byte[] toBytes(KV<K, V> obj) {
    if (obj != null) {
      byte[] keyBytes = keySerde.toBytes(obj.key);
      byte[] valueBytes = valueSerde.toBytes(obj.value);
      byte[] bytes = new byte[8 + keyBytes.length + 8 + valueBytes.length];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(keyBytes.length);
      byteBuffer.put(keyBytes);
      byteBuffer.putInt(valueBytes.length);
      byteBuffer.put(valueBytes);
      return byteBuffer.array();
    } else {
      return null;
    }
  }

  public Serde<K> getKeySerde() {
    return this.keySerde;
  }

  public Serde<V> getValueSerde() {
    return this.valueSerde;
  }
}