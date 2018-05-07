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

package org.apache.samza.operators.util;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterable;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements a {@link KeyValueStore} using an in-memory Java Map.
 * @param <K>  the type of key
 * @param <V>  the type of value
 *
 * TODO: This class is a stop-gap until we implement persistent store creation from TaskContext.
 *
 */
public class InternalInMemoryStore<K, V> implements KeyValueStore<K, V> {

  private final Map<K, V> map = new LinkedHashMap<>();

  @Override
  public V get(K key) {
    if (key == null) {
      throw new NullPointerException("Null key provided");
    }
    return map.get(key);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> values = new HashMap<>();
    for (K key : keys) {
      values.put(key, map.get(key));
    }
    return values;
  }

  @Override
  public void put(K key, V value) {
    if (key == null) {
      throw new NullPointerException("Null key provided");
    }
    map.put(key, value);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    for (Entry<K, V> entry : entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) {
    if (key == null) {
      throw new NullPointerException("Null key provided");
    }
    map.remove(key);
  }

  @Override
  public void deleteAll(List<K> keys) {
    for (K key : keys) {
      delete(key);
    }
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    throw new RuntimeException("not implemented.");
  }

  @Override
  public KeyValueIterable<K, V> iterate(K from, K to) {
    throw new UnsupportedOperationException("iterate() is not supported in " + InternalInMemoryStore.class.getName());
  }

  @Override
  public KeyValueIterator<K, V> all() {
    final Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
    return new KeyValueIterator<K, V>() {
      @Override
      public void close() {
        //not applicable
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Entry<K, V> next() {
        Map.Entry<K, V> kv = iterator.next();
        return new Entry<>(kv.getKey(), kv.getValue());
      }

      @Override
      public void remove() {
        iterator.remove();
      }

    };
  }

  @Override
  public void close() {
    //not applicable
  }

  @Override
  public void flush() {
    //not applicable
  }
}
