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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.samza.table.ReadableTable;

import com.google.common.base.Preconditions;


/**
 * A store backed readable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalStoreBackedReadableTable<K, V> implements ReadableTable<K, V> {

  protected KeyValueStore<K, V> kvStore;
  protected String tableId;

  /**
   * Constructs an instance of {@link LocalStoreBackedReadableTable}
   * @param kvStore the backing store
   */
  public LocalStoreBackedReadableTable(String tableId, KeyValueStore<K, V> kvStore) {
    Preconditions.checkArgument(tableId != null & !tableId.isEmpty() , "invalid tableId");
    Preconditions.checkNotNull(kvStore, "null KeyValueStore");
    this.tableId = tableId;
    this.kvStore = kvStore;
  }

  @Override
  public V get(K key) {
    return kvStore.get(key);
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    return keys.stream().collect(Collectors.toMap(k -> k, k -> kvStore.get(k)));
  }

  @Override
  public void close() {
    // The KV store is not closed here as it may still be needed by downstream operators,
    // it will be closed by the SamzaContainer
  }
}
