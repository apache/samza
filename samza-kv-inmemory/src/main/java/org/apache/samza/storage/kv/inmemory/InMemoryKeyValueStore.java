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
package org.apache.samza.storage.kv.inmemory;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueSnapshot;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;


/**
 * In-memory implementation of a {@link KeyValueStore}.
 *
 * This uses a {@link ConcurrentSkipListMap} to store the keys in order.
 */
public class InMemoryKeyValueStore implements KeyValueStore<byte[], byte[]> {
  private final KeyValueStoreMetrics metrics;
  private final ConcurrentSkipListMap<byte[], byte[]> underlying;

  /**
   * @param metrics A metrics instance to publish key-value store related statistics
   */
  public InMemoryKeyValueStore(KeyValueStoreMetrics metrics) {
    this.metrics = metrics;
    this.underlying = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
  }

  @Override
  public byte[] get(byte[] key) {
    this.metrics.gets().inc();
    Preconditions.checkArgument(key != null, "Null argument 'key' not allowed");
    byte[] found = this.underlying.get(key);
    if (found != null) {
      metrics.bytesRead().inc(found.length);
    }
    return found;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    this.metrics.puts().inc();
    Preconditions.checkArgument(key != null, "Null argument 'key' not allowed");
    if (value == null) {
      this.metrics.deletes().inc();
      this.underlying.remove(key);
    } else {
      this.metrics.bytesWritten().inc(key.length + value.length);
      this.underlying.put(key, value);
    }
  }

  @Override
  public void putAll(List<Entry<byte[], byte[]>> entries) {
    // TreeMap's putAll requires a map, so we'd need to iterate over all the entries anyway
    // to use it, in order to putAll here.  Therefore, just iterate here.
    for (Entry<byte[], byte[]> next : entries) {
      put(next.getKey(), next.getValue());
    }
  }

  @Override
  public void delete(byte[] key) {
    put(key, null);
  }

  @Override
  public KeyValueIterator<byte[], byte[]> range(byte[] from, byte[] to) {
    this.metrics.ranges().inc();
    Preconditions.checkArgument(from != null, "Null argument 'from' not allowed");
    Preconditions.checkArgument(to != null, "Null argument 'to' not allowed");
    return new InMemoryIterator(this.underlying.subMap(from, to).entrySet().iterator(), this.metrics);
  }

  @Override
  public KeyValueSnapshot<byte[], byte[]> snapshot(byte[] from, byte[] to) {
    // TODO: Bug: SAMZA-2564: does not satisfy immutability constraint, since entrySet is backed by the underlying map.
    // snapshot the underlying map
    Set<Map.Entry<byte[], byte[]>> entries = this.underlying.subMap(from, to).entrySet();
    return new KeyValueSnapshot<byte[], byte[]>() {
      @Override
      public KeyValueIterator<byte[], byte[]> iterator() {
        return new InMemoryIterator(entries.iterator(), metrics);
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public KeyValueIterator<byte[], byte[]> all() {
    this.metrics.alls().inc();
    return new InMemoryIterator(this.underlying.entrySet().iterator(), this.metrics);
  }

  @Override
  public Optional<Path> checkpoint(CheckpointId id) {
    // No checkpoint being persisted. State restores from Changelog.
    return Optional.empty();
  }

  @Override
  public void flush() {
    // No-op for In memory store.
    metrics.flushes().inc();
  }

  @Override
  public void close() {
  }

  private static class InMemoryIterator implements KeyValueIterator<byte[], byte[]> {
    private final Iterator<Map.Entry<byte[], byte[]>> iter;
    private final KeyValueStoreMetrics metrics;

    private InMemoryIterator(Iterator<Map.Entry<byte[], byte[]>> iter, KeyValueStoreMetrics metrics) {
      this.iter = iter;
      this.metrics = metrics;
    }

    @Override
    public boolean hasNext() {
      return this.iter.hasNext();
    }

    @Override
    public Entry<byte[], byte[]> next() {
      Map.Entry<byte[], byte[]> n = this.iter.next();
      if (n != null && n.getKey() != null) {
        this.metrics.bytesRead().inc(n.getKey().length);
      }
      if (n != null && n.getValue() != null) {
        this.metrics.bytesRead().inc(n.getValue().length);
      }
      return new Entry<>(n.getKey(), n.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("InMemoryKeyValueStore iterator doesn't support remove");
    }

    @Override
    public void close() {
    }
  }
}
