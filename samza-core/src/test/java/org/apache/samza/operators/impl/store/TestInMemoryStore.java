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

import com.google.common.primitives.UnsignedBytes;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterable;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An in-memory store that supports range queries.
 *
 * @param <K> the type of keys in the store
 * @param <V> the type of values in the store
 */
public class TestInMemoryStore<K, V> implements KeyValueStore<K, V> {
  private final ConcurrentSkipListMap<byte[], byte[]> map =
      new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
  private final Serde<K> keySerde;
  private final Serde<V> valSerde;

  public TestInMemoryStore(Serde<K> keySerde, Serde<V> valSerde) {
    this.keySerde = keySerde;
    this.valSerde = valSerde;
  }

  @Override
  public V get(K key) {
    byte[] keyBytes = keySerde.toBytes(key);
    byte[] valBytes = map.get(keyBytes);
    if (valBytes == null) return null;
    return valSerde.fromBytes(valBytes);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> results = new HashMap<>();
    for (K key : keys) {
      byte[] value = map.get(keySerde.toBytes(key));
      if (value != null) {
        results.put(key, valSerde.fromBytes(value));
      }
    }
    return results;
  }

  @Override
  public void put(K key, V value) {
    map.put(keySerde.toBytes(key), valSerde.toBytes(value));
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    for (Entry<K, V> entry : entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) {
    map.remove(keySerde.toBytes(key));
  }

  @Override
  public void deleteAll(List<K> keys) {
    for (K k : keys) {
      delete(k);
    }
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    ConcurrentNavigableMap<byte[], byte[]> values = map.subMap(keySerde.toBytes(from), keySerde.toBytes(to));
    return new InMemoryIterator(values.entrySet().iterator(), keySerde, valSerde);
  }

  @Override
  public KeyValueIterable<K, V> snapshot(K from, K to) {
    final ConcurrentNavigableMap<byte[], byte[]> values = map.subMap(keySerde.toBytes(from), keySerde.toBytes(to));
    return new KeyValueIterable<K, V>() {
      @Override
      public KeyValueIterator<K, V> iterator() {
        return new InMemoryIterator<>(values.entrySet().iterator(), keySerde, valSerde);
      }
    };
  }

  @Override
  public KeyValueIterator<K, V> all() {
    return new InMemoryIterator(map.entrySet().iterator(), keySerde, valSerde);
  }

  @Override
  public void close() {

  }

  @Override
  public void flush() {

  }

  private static class InMemoryIterator<K, V> implements KeyValueIterator<K, V> {

    Iterator<Map.Entry<byte[], byte[]>> wrapped;
    Serde<K> keySerializer;
    Serde<V> valSerializer;

    public InMemoryIterator(Iterator<Map.Entry<byte[], byte[]>> wrapped, Serde<K> keySerde, Serde<V> valSerde) {
      this.wrapped = wrapped;
      this.keySerializer = keySerde;
      this.valSerializer = valSerde;
    }

    @Override
    public void close() {
      //no op
    }

    @Override
    public boolean hasNext() {
      return wrapped.hasNext();
    }

    @Override
    public Entry<K, V> next() {
      Map.Entry<byte[], byte[]> next = wrapped.next();
      return new Entry<>(keySerializer.fromBytes(next.getKey()), valSerializer.fromBytes(next.getValue()));
    }
  }
}
