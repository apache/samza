package org.apache.samza.operators.impl;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implements a {@link KeyValueStore} using an in-memory Java Map.
 * @param <K>
 * @param <V>
 */
public class WindowStoreImpl<K,V> implements KeyValueStore<K,V> {

  final Map<K,V> map = new HashMap<>();

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K,V> values = new HashMap<>();
    for(K key : keys) {
      values.put(key, map.get(key));
    }
    return values;
  }

  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    for(Entry<K,V> entry : entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) {
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
