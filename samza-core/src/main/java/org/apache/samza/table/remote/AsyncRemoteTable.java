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
import org.apache.samza.table.AsyncReadWriteTable;


/**
 * A composable asynchronous table implementation that delegates read/write operations
 * to the underlying table read and write functions.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class AsyncRemoteTable<K, V> implements AsyncReadWriteTable<K, V> {

  private final TableReadFunction<K, V> readFn;
  private final TableWriteFunction<K, V> writeFn;

  public AsyncRemoteTable(TableReadFunction<K, V> readFn, TableWriteFunction<K, V> writeFn) {
    Preconditions.checkNotNull(readFn, "null readFn");
    this.readFn = readFn;
    this.writeFn = writeFn;
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    return readFn.getAsync(key);
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(List<K> keys) {
    return readFn.getAllAsync(keys);
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.putAsync(key, value);
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.putAllAsync(entries);
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.deleteAsync(key);
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    Preconditions.checkNotNull(writeFn, "null writeFn");
    return writeFn.deleteAllAsync(keys);
  }

  @Override
  public void init(Context context) {
    readFn.init(context);
    if (writeFn != null) {
      writeFn.init(context);
    }
  }

  @Override
  public void flush() {
    if (writeFn != null) {
      writeFn.flush();
    }
  }

  @Override
  public void close() {
    readFn.close();
    if (writeFn != null) {
      writeFn.close();
    }
  }
}
