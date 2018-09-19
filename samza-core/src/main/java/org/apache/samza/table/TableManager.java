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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.context.Context;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link TableManager} manages tables within a Samza task. For each table, it maintains
 * the {@link TableSpec}, the {@link TableProvider} and the {@link Table} instance.
 * It is used at execution for {@link org.apache.samza.container.TaskInstance} to retrieve
 * table instances for read/write operations.
 *
 * A {@link TableManager} is constructed from job configuration, the {@link TableSpec}
 * and {@link TableProvider} are constructed by processing the job configuration
 * during initialization. The {@link Table} is constructed when {@link #getTable(String)}
 * is called and cached.
 *
 * After a {@link TableManager} is constructed, local tables are associated with
 * local store instances created during {@link org.apache.samza.container.SamzaContainer}
 * initialization.
 *
 * Method {@link TableManager#getTable(String)} will throw {@link IllegalStateException},
 * if it's called before initialization.
 *
 */
public class TableManager {

  static public class TableCtx {
    private TableSpec tableSpec;
    private TableProvider tableProvider;
    private Table table;
  }

  private final Logger logger = LoggerFactory.getLogger(TableManager.class.getName());

  // tableId -> TableCtx
  private final Map<String, TableCtx> tableContexts = new HashMap<>();

  private boolean initialized;

  /**
   * Construct a table manager instance
   * @param config job configuration
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
   * Initialize table providers with container and task contexts
   * @param context context for the task
   */
  public void init(Context context) {
    Preconditions.checkNotNull(context, "Cannot pass null context");
    tableContexts.values().forEach(ctx -> ctx.tableProvider.init(context));
    initialized = true;
  }

  /**
   * Add a table to the table manager
   * @param tableSpec the table spec
   */
  private void addTable(TableSpec tableSpec) {
    if (tableContexts.containsKey(tableSpec.getId())) {
      throw new SamzaException("Table " + tableSpec.getId() + " already exists");
    }
    TableCtx ctx = new TableCtx();
    TableProviderFactory tableProviderFactory =
        Util.getObj(tableSpec.getTableProviderFactoryClassName(), TableProviderFactory.class);
    ctx.tableProvider = tableProviderFactory.getTableProvider(tableSpec);
    ctx.tableSpec = tableSpec;
    tableContexts.put(tableSpec.getId(), ctx);
  }

  /**
   * Shutdown the table manager, internally it shuts down all tables
   */
  public void close() {
    tableContexts.values().forEach(ctx -> ctx.tableProvider.close());
  }

  /**
   * Get a table instance
   * @param tableId Id of the table
   * @return table instance
   */
  public Table getTable(String tableId) {
    Preconditions.checkState(initialized, "TableManager has not been initialized.");

    TableCtx ctx = tableContexts.get(tableId);
    Preconditions.checkNotNull(ctx, "Unknown tableId " + tableId);

    if (ctx.table == null) {
      ctx.table = ctx.tableProvider.getTable();
    }
    return ctx.table;
  }
}
