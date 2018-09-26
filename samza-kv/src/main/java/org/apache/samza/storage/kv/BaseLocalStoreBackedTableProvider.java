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
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.BaseTableProvider;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;


/**
 * Base class for tables backed by Samza local stores. The backing stores are
 * injected during initialization of the table. Since the lifecycle
 * of the underlying stores are already managed by Samza container,
 * the table provider will not manage the lifecycle of the backing
 * stores.
 */
abstract public class BaseLocalStoreBackedTableProvider extends BaseTableProvider {
  public static final Pattern SYSTEM_STREAM_NAME_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  protected KeyValueStore kvStore;

  public BaseLocalStoreBackedTableProvider(TableSpec tableSpec) {
    super(tableSpec);
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {

    super.init(containerContext, taskContext);

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

  protected Map<String, String> generateCommonStoreConfig(Config jobConfig, Map<String, String> generatedConfig) {

    Map<String, String> storeConfig = new HashMap<>();

    // serde configurations for tables are generated at top level by JobNodeConfigurationGenerator and are included
    // in the global jobConfig. generatedConfig has all table specific configuration generated from TableSpec, such
    // as TableProviderFactory, sideInputs, etc.
    // Merge the global jobConfig and generatedConfig to get full access to configuration needed to create local
    // store configuration
    Map<String, String> mergedConfigMap = new HashMap<>(jobConfig);
    mergedConfigMap.putAll(generatedConfig);
    JobConfig mergedJobConfig = new JobConfig(new MapConfig(mergedConfigMap));
    JavaTableConfig tableConfig = new JavaTableConfig(mergedJobConfig);

    String keySerde = tableConfig.getKeySerde(tableSpec.getId());
    storeConfig.put(String.format(StorageConfig.KEY_SERDE(), tableSpec.getId()), keySerde);

    String valueSerde = tableConfig.getValueSerde(tableSpec.getId());
    storeConfig.put(String.format(StorageConfig.MSG_SERDE(), tableSpec.getId()), valueSerde);

    List<String> sideInputs = tableSpec.getSideInputs();
    if (sideInputs != null && !sideInputs.isEmpty()) {
      sideInputs.forEach(si -> Preconditions.checkState(isValidSystemStreamName(si), String.format(
          "Side input stream %s doesn't confirm to pattern %s", si, SYSTEM_STREAM_NAME_PATTERN)));
      String formattedSideInputs = String.join(",", sideInputs);
      storeConfig.put(String.format(JavaStorageConfig.SIDE_INPUTS, tableSpec.getId()), formattedSideInputs);
      storeConfig.put(String.format(JavaStorageConfig.SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE, tableSpec.getId()),
          SerdeUtils.serialize("Side Inputs Processor", tableSpec.getSideInputsProcessor()));
    }

    // Changelog configuration
    Boolean enableChangelog = Boolean.valueOf(
        tableSpec.getConfig().get(BaseLocalStoreBackedTableDescriptor.INTERNAL_ENABLE_CHANGELOG));
    if (enableChangelog) {
      String changelogStream = tableSpec.getConfig().get(BaseLocalStoreBackedTableDescriptor.INTERNAL_CHANGELOG_STREAM);
      if (StringUtils.isEmpty(changelogStream)) {
        changelogStream = String.format("%s-%s-table-%s", mergedJobConfig.getName().get(), mergedJobConfig.getJobId(),
            tableSpec.getId());
      }

      Preconditions.checkState(isValidSystemStreamName(changelogStream), String.format(
          "Changelog stream %s doesn't confirm to pattern %s", changelogStream, SYSTEM_STREAM_NAME_PATTERN));
      storeConfig.put(String.format(StorageConfig.CHANGELOG_STREAM(), tableSpec.getId()), changelogStream);

      String changelogReplicationFactor = tableSpec.getConfig().get(
          BaseLocalStoreBackedTableDescriptor.INTERNAL_CHANGELOG_REPLICATION_FACTOR);
      if (changelogReplicationFactor != null) {
        storeConfig.put(String.format(StorageConfig.CHANGELOG_REPLICATION_FACTOR(), tableSpec.getId()),
            changelogReplicationFactor);
      }
    }

    return storeConfig;
  }

  @Override
  public void close() {
    logger.info("Shutting down table provider for table " + tableSpec.getId());
  }

  private boolean isValidSystemStreamName(String name) {
    return StringUtils.isNotBlank(name) && SYSTEM_STREAM_NAME_PATTERN.matcher(name).matches();
  }
}
