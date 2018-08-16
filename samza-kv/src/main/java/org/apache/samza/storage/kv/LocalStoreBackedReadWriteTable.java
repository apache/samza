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
package org.apache.samza.storage.kv;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.task.TaskContext;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalStoreBackedReadWriteTable<K, V> extends LocalStoreBackedReadableTable<K, V>
    implements ReadWriteTable<K, V> {

  protected DefaultTableWriteMetrics writeMetrics;

  /**
   * Constructs an instance of {@link LocalStoreBackedReadWriteTable}
   * @param kvStore the backing store
   */
  public LocalStoreBackedReadWriteTable(String tableId, KeyValueStore kvStore) {
    super(tableId, kvStore);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    super.init(containerContext, taskContext);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      writeMetrics.numPuts.inc();
      long startNs = System.nanoTime();
      kvStore.put(key, value);
      writeMetrics.putNs.update(System.nanoTime() - startNs);
    } else {
      delete(key);
    }
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V value) {
    CompletableFuture<Void> future = new CompletableFuture();
    try {
      put(key, value);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    writeMetrics.numPutAlls.inc();
    long startNs = System.nanoTime();
    kvStore.putAll(entries);
    writeMetrics.putAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public CompletableFuture<Void> putAllAsync(List<Entry<K, V>> entries) {
    CompletableFuture<Void> future = new CompletableFuture();
    try {
      putAll(entries);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void delete(K key) {
    writeMetrics.numDeletes.inc();
    long startNs = System.nanoTime();
    kvStore.delete(key);
    writeMetrics.deleteNs.update(System.nanoTime() - startNs);
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    CompletableFuture<Void> future = new CompletableFuture();
    try {
      delete(key);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void deleteAll(List<K> keys) {
    writeMetrics.numDeleteAlls.inc();
    long startNs = System.nanoTime();
    kvStore.deleteAll(keys);
    writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(List<K> keys) {
    CompletableFuture<Void> future = new CompletableFuture();
    try {
      deleteAll(keys);
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public void flush() {
    writeMetrics.numFlushes.inc();
    long startNs = System.nanoTime();
    kvStore.flush();
    writeMetrics.flushNs.update(System.nanoTime() - startNs);
  }

}
