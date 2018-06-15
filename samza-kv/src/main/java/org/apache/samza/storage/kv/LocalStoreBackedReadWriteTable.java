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

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalStoreBackedReadWriteTable<K, V> extends LocalStoreBackedReadableTable<K, V>
    implements ReadWriteTable<K, V> {

  protected Timer putNs;
  protected Timer putAllNs;
  protected Timer deleteNs;
  protected Timer deleteAllNs;
  protected Timer flushNs;
  protected Counter numPuts;
  protected Counter numPutAlls;
  protected Counter numDeletes;
  protected Counter numDeleteAlls;
  protected Counter numFlushes;

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
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    putNs = tableMetricsUtil.newTimer("put-ns");
    putAllNs = tableMetricsUtil.newTimer("putAll-ns");
    deleteNs = tableMetricsUtil.newTimer("delete-ns");
    deleteAllNs = tableMetricsUtil.newTimer("deleteAll-ns");
    flushNs = tableMetricsUtil.newTimer("flush-ns");
    numPuts = tableMetricsUtil.newCounter("num-puts");
    numPutAlls = tableMetricsUtil.newCounter("num-putAlls");
    numDeletes = tableMetricsUtil.newCounter("num-deletes");
    numDeleteAlls = tableMetricsUtil.newCounter("num-deleteAlls");
    numFlushes = tableMetricsUtil.newCounter("num-flushes");
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      numPuts.inc();
      long startNs = System.nanoTime();
      kvStore.put(key, value);
      putNs.update(System.nanoTime() - startNs);
    } else {
      delete(key);
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    numPutAlls.inc();
    long startNs = System.nanoTime();
    kvStore.putAll(entries);
    putAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void delete(K key) {
    numDeletes.inc();
    long startNs = System.nanoTime();
    kvStore.delete(key);
    deleteNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void deleteAll(List<K> keys) {
    numDeleteAlls.inc();
    long startNs = System.nanoTime();
    kvStore.deleteAll(keys);
    deleteAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void flush() {
    numFlushes.inc();
    long startNs = System.nanoTime();
    kvStore.flush();
    flushNs.update(System.nanoTime() - startNs);
  }

}
