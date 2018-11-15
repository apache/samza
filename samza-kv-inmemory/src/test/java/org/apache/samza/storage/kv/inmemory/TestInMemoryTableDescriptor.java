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
import junit.framework.Assert;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.LocalTableProviderFactory;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.junit.Test;


public class TestInMemoryTableDescriptor {

  private static final String TABLE_ID = "t1";

  @Test
  public void testMinimal() {
    Map<String, String> tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertNotNull(tableConfig);
    Assert.assertEquals(4, tableConfig.size());
  }

  @Test
  public void testTableProviderFactoryConfig() {
    Map<String, String> tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertEquals(4, tableConfig.size());
    Assert.assertEquals(LocalTableProviderFactory.class.getName(),
        tableConfig.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, TABLE_ID)));
    Assert.assertEquals(InMemoryKeyValueStorageEngineFactory.class.getName(),
        tableConfig.get(String.format(StorageConfig.FACTORY(), TABLE_ID)));
  }

  private Config createJobConfig() {
    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put(String.format(JavaTableConfig.TABLE_KEY_SERDE, TABLE_ID), "serde-key");
    jobConfig.put(String.format(JavaTableConfig.TABLE_VALUE_SERDE, TABLE_ID), "serde-value");
    return new MapConfig(jobConfig);
  }

  private InMemoryTableDescriptor createTableDescriptor() {
    return new InMemoryTableDescriptor(TABLE_ID,
        new KVSerde(new NoOpSerde(), new NoOpSerde()));
  }
}
