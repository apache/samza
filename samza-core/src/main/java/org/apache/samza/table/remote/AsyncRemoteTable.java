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
package org.apache.samza.table.remote;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteUpdateTable;


/**
 * A composable asynchronous table implementation that delegates read/write operations
 * to the underlying table read and write functions.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <U> the type of the update applied to this table
 */
public class AsyncRemoteTable<K, V, U> implements AsyncReadWriteUpdateTable<K, V, U> {

  private final TableReadFunction<K, V> readFn;
  private final TableWriteFunction<K, V, U> writeFn;

  public AsyncRemoteTable(TableReadFunction<K, V> readFn, TableWriteFunction<K, V, U> writeFn) {
    Preconditions.checkArgument(writeFn != null || readFn != null,
        "Must have one of TableReadFunction or TableWriteFunction");
    this.readFn = readFn;
    this.writeFn = writeFn;
  }

  @Override
  public CompletableFuture<V> getAsync(K key, Object ... args) {
    Preconditions.checkNotNull(readFn, "null readFn");
    return args.length > 0
        ? readFn.getAsync(key, args)
        : readFn.getAsync(key);
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys, Object ... args) {
    Preconditions.checkNotNull(readFn, "null readFn");
    return args.length > 0
        ? readFn.getAllAsync(keys, args)
        : readFn.getAllAsync(keys);
  }

  @Override
  public <T> CompletableFuture<T> readAsync(int opId, Object... args) {
    Preconditions.checkNotNull(readFn, "null readFn");
    return readFn.readAsync(opId, args);
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V record, Object... args) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return args.length > 0
        ? writeFn.putAsync(key, record, args)
        : writeFn.putAsync(key, record);
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries, Object ... args) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return args.length > 0
        ? writeFn.putAllAsync(entries, args)
        : writeFn.putAllAsync(entries);
  }

  @Override
  public CompletableFuture<Void> updateAsync(K key, U update) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.updateAsync(key, update);
  }

  @Override
  public CompletableFuture<Void> updateAllAsync(List<Entry<K, U>> updates) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.updateAllAsync(updates);
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key, Object... args) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return args.length > 0
        ? writeFn.deleteAsync(key, args)
        : writeFn.deleteAsync(key);
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys, Object ... args) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return args.length > 0
        ? writeFn.deleteAllAsync(keys, args)
        : writeFn.deleteAllAsync(keys);
  }

  @Override
  public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.writeAsync(opId, args);
  }

  @Override
  public void init(Context context) {
    // Note: Initialization of table functions is done in {@link RemoteTable#init(Context)},
    //       as we need to pass in the reference to the top level table
  }

  @Override
  public void flush() {
    if (writeFn != null) {
      writeFn.flush();
    }
  }

  @Override
  public void close() {
    if (readFn != null) {
      readFn.close();
    }
    if (writeFn != null) {
      writeFn.close();
    }
  }
}
