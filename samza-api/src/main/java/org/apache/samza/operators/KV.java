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

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Objects;


/**
 * A key and value pair.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public final class KV<K, V> {
  public final K key;
  public final V value;

  public static <K, V> KV<K, V> of(K key, V value) {
    return new KV<>(key, value);
  }

  public KV(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof KV)) {
      return false;
    }
    KV<?, ?> otherKv = (KV<?, ?>) other;
    // deepEquals is used here for arrays. This impl comes from Beam.
    return Objects.deepEquals(this.key, otherKv.key)
        && Objects.deepEquals(this.value, otherKv.value);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[] {key, value});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(key)
        .addValue(value)
        .toString();
  }
}
