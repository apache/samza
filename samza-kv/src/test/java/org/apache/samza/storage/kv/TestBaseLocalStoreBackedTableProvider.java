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
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestBaseLocalStoreBackedTableProvider {

  @Test
  public void testInit() {
    Context context = mock(Context.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(context.getTaskContext()).thenReturn(taskContext);
    when(taskContext.getStore(any())).thenReturn(mock(KeyValueStore.class));
    when(taskContext.getTaskMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());

    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");

    TableProvider tableProvider = createTableProvider(tableSpec);
    tableProvider.init(context);
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
    Map<String, String> generatedConfig = new HashMap<>();
    generatedConfig.put(String.format(JavaTableConfig.TABLE_KEY_SERDE, "t1"), "ks1");
    generatedConfig.put(String.format(JavaTableConfig.TABLE_VALUE_SERDE, "t1"), "vs1");

    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");

    TableProvider tableProvider = createTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig(), generatedConfig);
    Assert.assertEquals("ks1", tableConfig.get(String.format(StorageConfig.KEY_SERDE(), "t1")));
    Assert.assertEquals("vs1", tableConfig.get(String.format(StorageConfig.MSG_SERDE(), "t1")));
  }

  @Test
  public void testChangelogDisabled() {
    TableSpec tableSpec = createTableDescriptor("t1")
        .getTableSpec();

    TableProvider tableProvider = createTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig(), new MapConfig());
    Assert.assertEquals(2, tableConfig.size());
    Assert.assertFalse(tableConfig.containsKey(String.format(StorageConfig.CHANGELOG_STREAM(), "t1")));
  }

  @Test
  public void testChangelogEnabled() {
    TableSpec tableSpec = createTableDescriptor("t1")
        .withChangelogEnabled()
        .getTableSpec();

    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put(JobConfig.JOB_NAME(), "test-job");
    jobConfig.put(JobConfig.JOB_ID(), "10");

    TableProvider tableProvider = createTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig(jobConfig), new MapConfig());
    Assert.assertEquals(3, tableConfig.size());
    Assert.assertEquals("test-job-10-table-t1", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM(), "t1"))));
  }

  @Test
  public void testChangelogEnabledWithCustomParameters() {
    TableSpec tableSpec = createTableDescriptor("t1")
        .withChangelogStream("my-stream")
        .withChangelogReplicationFactor(100)
        .getTableSpec();

    TableProvider tableProvider = createTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(new MapConfig(), new MapConfig());
    Assert.assertEquals(4, tableConfig.size());
    Assert.assertEquals("my-stream", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM(), "t1"))));
    Assert.assertEquals("100", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_REPLICATION_FACTOR(), "t1"))));
  }

  private TableProvider createTableProvider(TableSpec tableSpec) {
    return new BaseLocalStoreBackedTableProvider(tableSpec) {
      @Override
      public Map<String, String> generateConfig(Config jobConfig, Map<String, String> generatedConfig) {
        return generateCommonStoreConfig(jobConfig, generatedConfig);
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
