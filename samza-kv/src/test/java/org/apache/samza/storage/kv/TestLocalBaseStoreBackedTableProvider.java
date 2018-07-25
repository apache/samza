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

import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestLocalBaseStoreBackedTableProvider {

  private BaseLocalStoreBackedTableProvider tableProvider;

  @Before
  public void prepare() {
    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn("t1");
    tableProvider = new BaseLocalStoreBackedTableProvider(tableSpec) {
      @Override
      public Map<String, String> generateConfig(Map<String, String> config) {
        return generateCommonStoreConfig(config);
      }
    };
  }

  @Test
  public void testInit() {
    StorageEngine store = mock(KeyValueStorageEngine.class);
    SamzaContainerContext containerContext = mock(SamzaContainerContext.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getStore(any())).thenReturn(store);
    when(taskContext.getMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());
    tableProvider.init(containerContext, taskContext);
    Assert.assertNotNull(tableProvider.getTable());
  }

  @Test(expected = SamzaException.class)
  public void testInitFail() {
    Assert.assertNotNull(tableProvider.getTable());
  }

  @Test
  public void testGenerateCommonStoreConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(String.format(JavaTableConfig.TABLE_KEY_SERDE, "t1"), "ks1");
    config.put(String.format(JavaTableConfig.TABLE_VALUE_SERDE, "t1"), "vs1");

    Map<String, String> tableConfig = tableProvider.generateConfig(config);
    Assert.assertEquals("ks1", tableConfig.get(String.format(StorageConfig.KEY_SERDE(), "t1")));
    Assert.assertEquals("vs1", tableConfig.get(String.format(StorageConfig.MSG_SERDE(), "t1")));
  }
}
