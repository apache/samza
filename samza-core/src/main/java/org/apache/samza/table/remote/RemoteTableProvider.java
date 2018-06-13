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

package org.apache.samza.table.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provide for remote table instances
 */
public class RemoteTableProvider implements TableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteTableProvider.class);

  static final String READ_FN = "io.readFn";
  static final String WRITE_FN = "io.writeFn";
  static final String RATE_LIMITER = "io.ratelimiter";
  static final String READ_CREDIT_FN = "io.readCreditFn";
  static final String WRITE_CREDIT_FN = "io.writeCreditFn";

  private final TableSpec tableSpec;
  private final boolean readOnly;
  private final List<RemoteReadableTable<?, ?>> tables = new ArrayList<>();
  private SamzaContainerContext containerContext;
  private TaskContext taskContext;

  public RemoteTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
    readOnly = !tableSpec.getConfig().containsKey(WRITE_FN);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    this.containerContext = containerContext;
    this.taskContext = taskContext;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Table getTable() {
    RemoteReadableTable table;
    TableReadFunction<?, ?> readFn = getReadFn();
    RateLimiter rateLimiter = deserializeObject(RATE_LIMITER);
    if (rateLimiter != null) {
      rateLimiter.init(containerContext.config, taskContext);
    }
    CreditFunction<?, ?> readCreditFn = deserializeObject(READ_CREDIT_FN);
    if (readOnly) {
      table = new RemoteReadableTable(tableSpec.getId(), readFn, rateLimiter, readCreditFn);
    } else {
      CreditFunction<?, ?> writeCreditFn = deserializeObject(WRITE_CREDIT_FN);
      table = new RemoteReadWriteTable(tableSpec.getId(), readFn, getWriteFn(), rateLimiter, readCreditFn, writeCreditFn);
    }
    table.init(containerContext, taskContext);
    tables.add(table);
    return table;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {
    Map<String, String> tableConfig = new HashMap<>();

    // Insert table_id prefix to config entries
    tableSpec.getConfig().forEach((k, v) -> {
        String realKey = String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
        tableConfig.put(realKey, v);
      });

    LOG.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    tables.forEach(t -> t.close());
  }

  private <T> T deserializeObject(String key) {
    String entry = tableSpec.getConfig().getOrDefault(key, "");
    if (entry.isEmpty()) {
      return null;
    }
    return SerdeUtils.deserialize(key, entry);
  }

  private TableReadFunction<?, ?> getReadFn() {
    TableReadFunction<?, ?> readFn = deserializeObject(READ_FN);
    if (readFn != null) {
      readFn.init(containerContext.config, taskContext);
    }
    return readFn;
  }

  private TableWriteFunction<?, ?> getWriteFn() {
    TableWriteFunction<?, ?> writeFn = deserializeObject(WRITE_FN);
    if (writeFn != null) {
      writeFn.init(containerContext.config, taskContext);
    }
    return writeFn;
  }
}

