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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TableManager} manages tables within a Samza task. For each table, it maintains
 * the {@link TableSpec} and the {@link TableProvider}. It is used at execution for
 * {@link org.apache.samza.container.TaskInstance} to retrieve table instances for
 * read/write operations.
 *
 * A {@link TableManager} is constructed from job configuration, the {@link TableSpec}
 * and {@link TableProvider} are constructed by processing the job configuration.
 *
 * After a {@link TableManager} is constructed, local tables are associated with
 * local store instances created during {@link org.apache.samza.container.SamzaContainer}
 * initialization.
 *
 * Method {@link TableManager#getTable(String)} will throw {@link IllegalStateException},
 * if it's called before initialization.
 *
 * For store backed tables, the list of stores must be injected into the constructor.
 */
public class TableManager {

  static public class TableCtx {
    private TableSpec tableSpec;
    private TableProvider tableProvider;
  }

  private final Logger logger = LoggerFactory.getLogger(TableManager.class.getName());

  // tableId -> TableCtx
  private final Map<String, TableCtx> tables = new HashMap<>();

  private boolean localTablesInitialized;

  /**
   * Construct a table manager instance
   * @param config the job configuration
   * @param serdes Serde instances for tables
   */
  public TableManager(Config config, Map<String, Serde<Object>> serdes) {

    new JavaTableConfig(config).getTableIds().forEach(tableId -> {

        // Construct the table provider
        String tableProviderFactory = config.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableId));

        // Construct the KVSerde
        JavaTableConfig tableConfig = new JavaTableConfig(config);
        KVSerde serde = KVSerde.of(
            serdes.get(tableConfig.getKeySerde(tableId)),
            serdes.get(tableConfig.getValueSerde(tableId)));

        TableSpec tableSpec = new TableSpec(tableId, serde, tableProviderFactory,
            config.subset(String.format(JavaTableConfig.TABLE_ID_PREFIX, tableId) + "."));

        addTable(tableSpec);

        logger.info("Added table " + tableSpec.getId());
      });
  }

  /**
   * Initialize all local table
   * @param stores stores created locally
   */
  public void initLocalTables(Map<String, StorageEngine> stores) {
    tables.values().forEach(ctx -> {
        if (ctx.tableProvider instanceof LocalStoreBackedTableProvider) {
          StorageEngine store = stores.get(ctx.tableSpec.getId());
          if (store == null) {
            throw new SamzaException(String.format(
                "Backing store for table %s was not injected by SamzaContainer",
                ctx.tableSpec.getId()));
          }
          ((LocalStoreBackedTableProvider) ctx.tableProvider).init(store);
        }
      });

    localTablesInitialized = true;
  }

  /**
   * Add a table to the table manager
   * @param tableSpec the table spec
   */
  private void addTable(TableSpec tableSpec) {
    if (tables.containsKey(tableSpec.getId())) {
      throw new SamzaException("Table " + tableSpec.getId() + " already exists");
    }
    TableCtx ctx = new TableCtx();
    TableProviderFactory tableProviderFactory = Util.getObj(tableSpec.getTableProviderFactoryClassName());
    ctx.tableProvider = tableProviderFactory.getTableProvider(tableSpec);
    ctx.tableSpec = tableSpec;
    tables.put(tableSpec.getId(), ctx);
  }

  /**
   * Start the table manager, internally it starts all tables
   */
  public void start() {
    tables.values().forEach(ctx -> ctx.tableProvider.start());
  }

  /**
   * Shutdown the table manager, internally it shuts down all tables
   */
  public void shutdown() {
    tables.values().forEach(ctx -> ctx.tableProvider.stop());
  }

  /**
   * Get a table instance
   * @param tableId Id of the table
   * @return table instance
   */
  public Table getTable(String tableId) {
    if (!localTablesInitialized) {
      throw new IllegalStateException("Local tables in TableManager not initialized.");
    }
    return tables.get(tableId).tableProvider.getTable();
  }
}
