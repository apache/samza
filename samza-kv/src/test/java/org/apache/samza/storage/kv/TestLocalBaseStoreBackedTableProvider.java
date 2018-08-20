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
import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class TestLocalBaseStoreBackedTableProvider {

  @Test
  public void testInit() {
    StorageEngine store = mock(KeyValueStorageEngine.class);
    SamzaContainerContext containerContext = mock(SamzaContainerContext.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getStore(any())).thenReturn(store);
    when(taskContext.getMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());

    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");

    TableProvider tableProvider = createTableProvider(tableSpec);
    tableProvider.init(containerContext, taskContext);
    Assert.assertNotNull(tableProvider.getTable());
  }

  @Test(expected = SamzaException.class)
  public void testInitFail() {
    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");
    TableProvider tableProvider = createTableProvider(tableSpec);
    Assert.assertNotNull(tableProvider.getTable());
  }

  @Test
  public void testGenerateCommonStoreConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(String.format(JavaTableConfig.TABLE_KEY_SERDE, "t1"), "ks1");
    config.put(String.format(JavaTableConfig.TABLE_VALUE_SERDE, "t1"), "vs1");

    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");

    TableProvider tableProvider = createTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(config);
    Assert.assertEquals("ks1", tableConfig.get(String.format(StorageConfig.KEY_SERDE(), "t1")));
    Assert.assertEquals("vs1", tableConfig.get(String.format(StorageConfig.MSG_SERDE(), "t1")));
  }

  @Test
  public void testChangelogDisabled() {
    TableSpec tableSpec = createTableDescriptor("t1")
        .withChangelogDisabled()
        .getTableSpec();

    TableProvider tableProvider = createTableProvider(tableSpec);
    tableProvider.init(new MapConfig(new MapConfig()));

    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig());
    Assert.assertEquals(2, tableConfig.size());
    Assert.assertFalse(tableConfig.containsKey(String.format(StorageConfig.CHANGELOG_STREAM(), "t1")));
  }

  @Test
  public void testChangelogEnabled() {
    TableSpec tableSpec = createTableDescriptor("$1")
        .getTableSpec();

    Map<String, String> config = new HashMap<>();
    config.put(JobConfig.JOB_NAME(), "test-job");
    config.put(JobConfig.JOB_ID(), "10");

    TableProvider tableProvider = createTableProvider(tableSpec);
    tableProvider.init(new MapConfig(config));

    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig());
    Assert.assertEquals(3, tableConfig.size());
    Assert.assertEquals("test-job-10-table--1", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM(), "$1"))));
  }

  @Test
  public void testChangelogEnabledWithCustomParameters() {
    TableSpec tableSpec = createTableDescriptor("t1")
        .withChangelogStream("my$tream")
        .withChangelogReplicationFactor(100)
        .getTableSpec();

    TableProvider tableProvider = createTableProvider(tableSpec);
    tableProvider.init(new MapConfig(new HashMap<>()));

    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig());
    Assert.assertEquals(4, tableConfig.size());
    Assert.assertEquals("my-tream", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM(), "t1"))));
    Assert.assertEquals("100", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_REPLICATION_FACTOR(), "t1"))));
  }

  private TableProvider createTableProvider(TableSpec tableSpec) {
    return new BaseLocalStoreBackedTableProvider(tableSpec) {
      @Override
      public Map<String, String> generateConfig(Map<String, String> config) {
        return generateCommonStoreConfig(config);
      }
    };
  }

  private BaseLocalStoreBackedTableDescriptor createTableDescriptor(String tableId) {
    return new BaseLocalStoreBackedTableDescriptor(tableId) {
      @Override
      public TableSpec getTableSpec() {
        validate();
        Map<String, String> tableSpecConfig = new HashMap<>();
        generateTableSpecConfig(tableSpecConfig);
        return new TableSpec(tableId, serde, null, tableSpecConfig,
            sideInputs, sideInputsProcessor);
      }
    };
  }
}
