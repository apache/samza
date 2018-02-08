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
package org.apache.samza.table;

import java.util.List;
import java.util.Map;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.KV;


/**
 *
 * A table that supports get by one or more keys
 *
 * @param <K> the type of the record key in this table
 * @param <V> the type of the record value in this table
 */
@InterfaceStability.Unstable
public interface ReadableTable<K, V> extends Table<KV<K, V>> {

  /**
   * Gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @return if found, the value associated with the specified {@code key}; otherwise, {@code null}.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  V get(K key);

  /**
   * Gets the values with which the specified {@code keys} are associated.
   *
   * @param keys the keys with which the associated values are to be fetched.
   * @return a map of the keys that were found and their respective values.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  Map<K, V> getAll(List<K> keys);

  /**
   * Close the table and release any resources acquired
   */
  void close();

}
