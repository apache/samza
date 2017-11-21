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

package org.apache.samza.storage.kv.inmemory;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import junit.framework.Assert;


public class TestInMemoryTableProvider {
  @Test
  public void testGenerateConfig() {
    Map<String, String> tableSpecConfig = new HashMap<>();
    tableSpecConfig.put("inmemory.c1", "c1-value");
    tableSpecConfig.put("inmemory.c2", "c2-value");
    tableSpecConfig.put("c3", "c3-value");
    tableSpecConfig.put("c4", "c4-value");

    TableSpec tableSpec = new TableSpec("t1", KVSerde.of(new IntegerSerde(), new IntegerSerde()),
        "my-table-provider-factory", tableSpecConfig);

    Map<String, String> config = new HashMap<>();
    config.put(String.format(JavaTableConfig.TABLE_KEY_SERDE, "t1"), "ks1");
    config.put(String.format(JavaTableConfig.TABLE_VALUE_SERDE, "t1"), "vs1");

    TableProvider tableProvider = new InMemoryTableProvider(tableSpec);
    Map<String, String> tableConfig = tableProvider.generateConfig(config);

    Assert.assertEquals("ks1", tableConfig.get(String.format(StorageConfig.KEY_SERDE(), "t1")));
    Assert.assertEquals("vs1", tableConfig.get(String.format(StorageConfig.MSG_SERDE(), "t1")));
    Assert.assertEquals(
        InMemoryKeyValueStorageEngineFactory.class.getName(),
        tableConfig.get(String.format(StorageConfig.FACTORY(), "t1")));
    Assert.assertEquals("c1-value", tableConfig.get("stores.t1.c1"));
    Assert.assertEquals("c2-value", tableConfig.get("stores.t1.c2"));
    Assert.assertEquals("c3-value", tableConfig.get("tables.t1.c3"));
    Assert.assertEquals("c4-value", tableConfig.get("tables.t1.c4"));
  }
}
