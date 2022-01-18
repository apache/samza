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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.context.Context;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * A {@link TableManager} manages tables within a Samza task. For each table, it maintains
 * the {@link TableProvider} and the {@link Table} instance.
 * It is used at execution for {@link org.apache.samza.container.TaskInstance} to retrieve
 * table instances for read/write operations.
 *
 * A {@link TableManager} is constructed from job configuration, the
 * {@link TableProvider} are constructed by processing the job configuration
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

  static class TableCtx {
    private TableProvider tableProvider;
    private ReadWriteUpdateTable table;
  }

  private final Logger logger = LoggerFactory.getLogger(TableManager.class.getName());

  // tableId -> TableCtx
  private final Map<String, TableCtx> tableContexts = new HashMap<>();

  private boolean initialized;

  /**
   * Construct a table manager instance
   * @param config job configuration
   */
  public TableManager(Config config) {
    new JavaTableConfig(config).getTableIds().forEach(tableId -> {
      addTable(tableId, config);
      logger.debug("Added table " + tableId);
    });
    logger.info(String.format("Added %d tables", tableContexts.size()));
  }

  /**
   * Initialize table providers with container and task contexts
   * @param context context for the task
   */
  public void init(Context context) {
    tableContexts.values().forEach(ctx -> ctx.tableProvider.init(context));
    initialized = true;
  }

  private void addTable(String tableId, Config config) {
    if (tableContexts.containsKey(tableId)) {
      throw new SamzaException("Table " + tableId + " already exists");
    }
    JavaTableConfig tableConfig = new JavaTableConfig(config);
    String providerFactoryClassName = tableConfig.getTableProviderFactory(tableId);
    TableProviderFactory tableProviderFactory =
        ReflectionUtil.getObj(providerFactoryClassName, TableProviderFactory.class);
    TableCtx ctx = new TableCtx();
    ctx.tableProvider = tableProviderFactory.getTableProvider(tableId);
    tableContexts.put(tableId, ctx);
  }

  /**
   * Flush all tables
   */
  public void flush() {
    tableContexts.values().stream()
        .filter(ctx -> ctx.table != null)
        .forEach(ctx -> ctx.table.flush());
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
  public ReadWriteUpdateTable getTable(String tableId) {
    Preconditions.checkState(initialized, "TableManager has not been initialized.");

    TableCtx ctx = tableContexts.get(tableId);
    Preconditions.checkNotNull(ctx, "Unknown tableId " + tableId);

    if (ctx.table == null) {
      ctx.table = ctx.tableProvider.getTable();
    }
    return ctx.table;
  }
}
