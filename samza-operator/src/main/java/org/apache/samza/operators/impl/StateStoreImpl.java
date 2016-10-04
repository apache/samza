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
package org.apache.samza.operators.impl;

import org.apache.samza.operators.api.data.Message;
import org.apache.samza.operators.api.internal.Operators.StoreFunctions;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;


/**
 * The base class for all state stores
 */
public class StateStoreImpl<M extends Message, SK, SS> {
  private final String storeName;
  private final StoreFunctions<M, SK, SS> storeFunctions;
  private KeyValueStore<SK, SS> kvStore = null;

  public StateStoreImpl(StoreFunctions<M, SK, SS> store, String storeName) {
    this.storeFunctions = store;
    this.storeName = storeName;
  }

  public void init(TaskContext context) {
    this.kvStore = (KeyValueStore<SK, SS>) context.getStore(this.storeName);
  }

  public Entry<SK, SS> getState(M m) {
    SK key = this.storeFunctions.getStoreKeyFinder().apply(m);
    SS state = this.kvStore.get(key);
    return new Entry<>(key, state);
  }

  public Entry<SK, SS> updateState(M m, Entry<SK, SS> oldEntry) {
    SS newValue = this.storeFunctions.getStateUpdater().apply(m, oldEntry.getValue());
    this.kvStore.put(oldEntry.getKey(), newValue);
    return new Entry<>(oldEntry.getKey(), newValue);
  }
}
