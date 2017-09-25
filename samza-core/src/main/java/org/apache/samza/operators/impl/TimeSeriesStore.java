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
 *
 */

package org.apache.samza.operators.impl;

import org.apache.samza.storage.kv.ClosableIterator;
import org.apache.samza.storage.kv.KV;

/**
 * A key-value store that allows entries to be queried and stored based on time ranges.
 *
 * Operations on the store can be invoked from multiple threads. So, implementations are expected to be thread-safe.
 *
 * @param <K>, the type of key in the store
 * @param <V>, the type of value in the store
 */

public interface TimeSeriesStore<K, V> {

  ClosableIterator<KV<V, Long>> get(K key, Long startTimestamp, Long endTimeStamp);

  void put(K key, V val, Long timeStamp);

  void remove(K key, Long startTimestamp, Long endTimeStamp);
}
