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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.table.StoreBackedTableProvider;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for tables backed by Samza stores, see {@link StoreBackedTableProvider}.
 */
abstract public class BaseStoreBackedTableProvider implements StoreBackedTableProvider {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected final TableSpec tableSpec;

  protected KeyValueStore kvStore;

  public BaseStoreBackedTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  @Override
  public void init(StorageEngine store) {
    kvStore = (KeyValueStore) store;
    logger.info("Initialized backing store for table " + tableSpec.getId());
  }

  @Override
  public Table getTable() {
    if (kvStore == null) {
      throw new SamzaException("Store not initialized for table " + tableSpec.getId());
    }
    return new StoreBackedReadWriteTable(kvStore);
  }

  @Override
  public void start() {
    logger.info("Starting table provider for table " + tableSpec.getId());
  }

  @Override
  public void stop() {
    logger.info("Stopping table provider for table " + tableSpec.getId());
  }

  protected Map<String, String> generateCommonStoreConfig(Map<String, String> config) {

    Map<String, String> storeConfig = new HashMap<>();

    // We assume the configuration for serde are already generated for this table,
    // so we simply carry them over to store configuration.
    //
    JavaTableConfig tableConfig = new JavaTableConfig(new MapConfig(config));

    String keySerde = tableConfig.getKeySerde(tableSpec.getId());
    storeConfig.put(String.format(StorageConfig.KEY_SERDE(), tableSpec.getId()), keySerde);

    String valueSerde = tableConfig.getValueSerde(tableSpec.getId());
    storeConfig.put(String.format(StorageConfig.MSG_SERDE(), tableSpec.getId()), valueSerde);

    return storeConfig;
  }
}
