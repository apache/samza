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

import org.apache.samza.table.ReadWriteTable;


/**
 * A store backed readable and writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class LocalStoreBackedReadWriteTable<K, V> extends LocalStoreBackedReadableTable<K, V>
    implements ReadWriteTable<K, V> {

  /**
   * Constructs an instance of {@link LocalStoreBackedReadWriteTable}
   * @param kvStore the backing store
   */
  public LocalStoreBackedReadWriteTable(String tableId, KeyValueStore kvStore) {
    super(tableId, kvStore);
  }

  @Override
  public void put(K key, V value) {
    if (value != null) {
      kvStore.put(key, value);
    } else {
      delete(key);
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    entries.forEach(e -> kvStore.put(e.getKey(), e.getValue()));
  }

  @Override
  public void delete(K key) {
    kvStore.delete(key);
  }

  @Override
  public void deleteAll(List<K> keys) {
    keys.forEach(k -> kvStore.delete(k));
  }

  @Override
  public void flush() {
    kvStore.flush();
  }

}
