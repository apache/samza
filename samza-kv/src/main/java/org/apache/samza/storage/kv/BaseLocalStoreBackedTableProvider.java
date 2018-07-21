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
import java.util.List;
import java.util.Map;

import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Base class for tables backed by Samza local stores. The backing stores are
 * injected during initialization of the table. Since the lifecycle
 * of the underlying stores are already managed by Samza container,
 * the table provider will not manage the lifecycle of the backing
 * stores.
 */
abstract public class BaseLocalStoreBackedTableProvider implements TableProvider {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected final TableSpec tableSpec;

  protected KeyValueStore kvStore;

  protected SamzaContainerContext containerContext;

  protected TaskContext taskContext;

  public BaseLocalStoreBackedTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    this.containerContext = containerContext;
    this.taskContext = taskContext;

    Preconditions.checkNotNull(this.taskContext, "Must specify task context for local tables.");

    kvStore = (KeyValueStore) taskContext.getStore(tableSpec.getId());

    if (kvStore == null) {
      throw new SamzaException(String.format(
          "Backing store for table %s was not injected by SamzaContainer", tableSpec.getId()));
    }

    logger.info("Initialized backing store for table " + tableSpec.getId());
  }

  @Override
  public Table getTable() {
    if (kvStore == null) {
      throw new SamzaException("Store not initialized for table " + tableSpec.getId());
    }
    ReadableTable table = new LocalStoreBackedReadWriteTable(tableSpec.getId(), kvStore);
    table.init(containerContext, taskContext);
    return table;
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

    List<String> sideInputs = tableSpec.getSideInputs();

    // We already validate up the chain on the invariant that the side input processor is present in the case of
    // side inputs
    if (sideInputs != null && !sideInputs.isEmpty()) {
      String formattedSideInputs = sideInputs.stream()
          .collect(Collectors.joining(","));

      storeConfig.put(String.format(JavaStorageConfig.SIDE_INPUTS, tableSpec.getId()), formattedSideInputs);
      storeConfig.put(String.format(JavaStorageConfig.SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE, tableSpec.getId()),
          SerdeUtils.serialize("side input processor", tableSpec.getSideInputProcessor()));
    }

    return storeConfig;
  }

  @Override
  public void close() {
    logger.info("Shutting down table provider for table " + tableSpec.getId());
  }
}
