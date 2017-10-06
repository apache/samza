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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TableManager} manages all tables of a Samza job. For each table, it maintains
 * the {@link TableSpec} and the {@link TableProvider}. It is used at execution for
 * {@link org.apache.samza.container.TaskInstance} to retrieve table instances for
 * read/write operations.
 *
 * A {@link TableManager} is constructed from job configuration, the {@link TableSpec}
 * {@link TableProvider} are constructed by processing the job configuration.
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
  private Map<String, TableCtx> tables = new HashMap<>();

  /**
   * Construct a table manager instance
   * @param config the job configuration
   */
  public TableManager(Config config) {

    SerializableSerde<Serde> serializableSerde = new SerializableSerde();

    new JavaTableConfig(config).getTableIds().forEach(tableId -> {

        String tableProviderFactory = config.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableId));

        // Reconstruct the key Serde
        String keySerdeKey = String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE(),
            config.get(String.format(JavaTableConfig.TABLE_KEY_SERDE, tableId)));
        String keySerdeValue = config.get(keySerdeKey);
        Serde keySerde = serializableSerde.fromBytes(Base64.getDecoder().decode(keySerdeValue));

        // Reconstruct the value Serde
        String valueSerdeKey = String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE(),
            config.get(String.format(JavaTableConfig.TABLE_VALUE_SERDE, tableId)));
        String valueSerdeValue = config.get(valueSerdeKey);
        Serde valueSerde = serializableSerde.fromBytes(Base64.getDecoder().decode(valueSerdeValue));

        TableSpec tableSpec = new TableSpec(tableId, keySerde, valueSerde, tableProviderFactory,
            config.subset(String.format(JavaTableConfig.TABLE_ID_PREFIX, tableId) + "."));

        addTable(tableSpec);

        logger.info("Added table " + tableSpec.getId());
      });
  }

  /**
   * Construct a table manager instance
   * @param config the job configuration
   * @param stores the stores of the job
   */
  public TableManager(Config config, Map<String, StorageEngine> stores) {

    this(config);

    tables.values().forEach(ctx -> {
        if (ctx.tableProvider instanceof StoreBackedTableProvider) {
          StorageEngine store = stores.get(ctx.tableSpec.getId());
          if (store == null) {
            throw new SamzaException(String.format(
                "Backing store for table %s was not injected by SamzaContainer",
                ctx.tableSpec.getId()));
          }
          ((StoreBackedTableProvider) ctx.tableProvider).init(store);
        }
      });
  }

  /**
   * Add a table to the table manager
   * @param tableDescriptor the table descriptor
   */
  public void addTable(TableDescriptor tableDescriptor) {
    addTable(tableDescriptor.getTableSpec());
  }

  /**
   * Add a table to the table manager
   * @param tableSpec the table spec
   */
  public void addTable(TableSpec tableSpec) {
    if (tables.containsKey(tableSpec.getId())) {
      throw new SamzaException("Table " + tableSpec.getId() + " already exists");
    }
    TableCtx ctx = new TableCtx();
    TableProviderFactory tableProviderFactory = Util.getObj(tableSpec.getTableProviderFactory());
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
    tables.values().forEach(ctx -> ctx.tableProvider.shutdown());
  }

  /**
   * Get a table instance
   * @param tableId Id of the table
   * @return table instance
   */
  public Table getTable(String tableId) {
    return tables.get(tableId).tableProvider.getTable();
  }

  /**
   * Get a table instance
   * @param tableSpec the table spec
   * @return table instance
   */
  public Table getTable(TableSpec tableSpec) {
    return getTable(tableSpec.getId());
  }
}
