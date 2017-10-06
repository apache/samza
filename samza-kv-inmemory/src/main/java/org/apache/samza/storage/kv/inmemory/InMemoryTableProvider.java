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
package org.apache.samza.storage.kv.inmemory;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.storage.kv.BaseStoreBackedTableProvider;
import org.apache.samza.table.TableSpec;


/**
 * Table provider of an in-memory table
 */
public class InMemoryTableProvider extends BaseStoreBackedTableProvider {

  public InMemoryTableProvider(TableSpec tableSpec) {
    super(tableSpec);
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {

    Map<String, String> tableConfig = new HashMap<>();

    // Store factory configuration
    tableConfig.put(String.format(
        StorageConfig.FACTORY(), tableSpec.getId()),
        InMemoryKeyValueStorageEngineFactory.class.getName());

    // Common store configuration
    tableConfig.putAll(generateCommonStoreConfig(config));

    // Rest of the configuration
    tableSpec.getConfig().forEach((k, v) -> {
      String realKey = k.startsWith("inmemory.") ?
          String.format("stores.%s", tableSpec.getId()) + "." + k.substring("inmemory.".length())
        : String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
      tableConfig.put(realKey, v);
    });

    logger.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }

}
