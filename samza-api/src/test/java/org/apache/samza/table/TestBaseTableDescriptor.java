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

import java.util.Map;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.junit.Assert;
import org.junit.Test;


public class TestBaseTableDescriptor {

  private static final String TABLE_ID = "t1";

  @Test
  public void testMinimal() {
    Map<String, String> tableConfig = createTableDescriptor(TABLE_ID)
        .toConfig(new MapConfig());
    Assert.assertEquals(1, tableConfig.size());
  }

  @Test
  public void testProviderFactoryConfig() {
    Map<String, String> tableConfig = createTableDescriptor(TABLE_ID)
        .toConfig(new MapConfig());
    Assert.assertEquals(1, tableConfig.size());
    assertEquals("my-factory", "provider.factory", TABLE_ID, tableConfig);
  }

  @Test
  public void testCustomConfig() {
    Map<String, String> tableConfig = createTableDescriptor(TABLE_ID)
        .withConfig("abc", "xyz")
        .toConfig(new MapConfig());
    Assert.assertEquals(2, tableConfig.size());
    Assert.assertEquals("xyz", tableConfig.get("abc"));
  }

  private BaseTableDescriptor createTableDescriptor(String tableId) {
    return new BaseTableDescriptor(tableId) {
      public String getProviderFactoryClassName() {
        return "my-factory";
      }
    };
  }

  private void assertEquals(String expectedValue, String key, String tableId, Map<String, String> config) {
    String realKey = JavaTableConfig.buildKey(tableId, key);
    Assert.assertEquals(expectedValue, config.get(realKey));
  }
}
