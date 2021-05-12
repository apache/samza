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

package org.apache.samza.storage;

import java.util.Map;
import java.util.Set;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.task.MessageCollector;

/**
 * Provides the required for Kafka Changelog restore managers
 */
public class KafkaChangelogRestoreParams {
  private final Map<String, SystemConsumer> storeConsumers;
  private final Map<String, StorageEngine> inMemoryStores;
  private final Map<String, SystemAdmin> systemAdmins;
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories;
  private final Map<String, Serde<Object>> serdes;
  private final MessageCollector collector;
  private final Set<String> storeNames;

  public KafkaChangelogRestoreParams(
      Map<String, SystemConsumer> storeConsumers,
      Map<String, StorageEngine> inMemoryStores,
      Map<String, SystemAdmin> systemAdmins,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes,
      MessageCollector collector,
      Set<String> storeNames) {
    this.storeConsumers = storeConsumers;
    this.inMemoryStores = inMemoryStores;
    this.systemAdmins = systemAdmins;
    this.storageEngineFactories = storageEngineFactories;
    this.serdes = serdes;
    this.collector = collector;
    this.storeNames = storeNames;
  }

  public Map<String, SystemConsumer> getStoreConsumers() {
    return storeConsumers;
  }

  public Map<String, StorageEngine> getInMemoryStores() {
    return inMemoryStores;
  }

  public Map<String, SystemAdmin> getSystemAdmins() {
    return systemAdmins;
  }

  public Map<String, StorageEngineFactory<Object, Object>> getStorageEngineFactories() {
    return storageEngineFactories;
  }

  public Map<String, Serde<Object>> getSerdes() {
    return serdes;
  }

  public MessageCollector getCollector() {
    return collector;
  }

  public Set<String> getStoreNames() {
    return storeNames;
  }
}
