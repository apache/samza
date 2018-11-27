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
package org.apache.samza.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class TestJavaTableConfig {
  @Test
  public void testGetTableIds() {
    Set<String> ids = Sets.newHashSet("t1", "t2");
    Map<String, String> map = ids.stream()
        .map(id -> String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, id))
        .collect(Collectors.toMap(key -> key, key -> key + "-provider-factory"));
    JavaTableConfig tableConfig = new JavaTableConfig(new MapConfig(map));

    assertEquals(2, tableConfig.getTableIds().size());

    ids.removeAll(tableConfig.getTableIds());
    assertTrue(ids.isEmpty());
  }

  @Test
  public void testGetTableProperties() {
    Map<String, String> map = new HashMap<>();
    map.put("stores.t1.key.serde", "key-serde");
    map.put("stores.t1.msg.serde", "msg-serde");
    map.put("tables.t1.provider.factory", "t1-provider-factory");
    JavaTableConfig tableConfig = new JavaTableConfig(new MapConfig(map));
    assertEquals("t1-provider-factory", tableConfig.getTableProviderFactory("t1"));
    assertEquals("key-serde", tableConfig.getKeySerde("t1"));
    assertEquals("msg-serde", tableConfig.getMsgSerde("t1"));
  }

  @Test
  public void testBuildKey() {
    String key = JavaTableConfig.buildKey("t1", "abc");
    Assert.assertEquals("tables.t1.abc", key);
  }

  @Test
  public void testGetForTable() {
    Map<String, String> map = new HashMap<>();
    map.put(JavaTableConfig.buildKey("t1", "abc"), "xyz");
    JavaTableConfig tableConfig = new JavaTableConfig(new MapConfig(map));
    Assert.assertEquals("xyz", tableConfig.getForTable("t1", "abc"));
    Assert.assertNull(tableConfig.getForTable("t1", "aaa"));
    Assert.assertEquals("xyz", tableConfig.getForTable("t1", "aaa", "xyz"));
    Assert.assertNull(tableConfig.getForTable("tt", "abc"));
    Assert.assertEquals("xyz", tableConfig.getForTable("tt", "abc", "xyz"));
  }
}
