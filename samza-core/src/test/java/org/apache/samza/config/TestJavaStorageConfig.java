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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestJavaStorageConfig {

  @Test
  public void testStorageConfig() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("stores.test.factory", "testFactory");
    map.put("stores.test.changelog", "testSystem.testChangelog");
    map.put("stores.test.key.serde", "string");
    map.put("stores.test.msg.serde", "integer");
    JavaStorageConfig config = new JavaStorageConfig(new MapConfig(map));

    assertEquals("testFactory", config.getStorageFactoryClassName("test"));
    assertEquals("testSystem.testChangelog", config.getChangelogStream("test"));
    assertEquals("string", config.getStorageKeySerde("test"));
    assertEquals("integer", config.getStorageMsgSerde("test"));
    assertEquals("test", config.getStoreNames().get(0));
  }


  @Test
  public void testIsChangelogSystemSetting() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("stores.store1.changelog", "system1.stream1");
    configMap.put("job.changelog.system", "system2");
    configMap.put("stores.store2.changelog", "stream2");

    JavaStorageConfig config = new JavaStorageConfig(new MapConfig(configMap));

    assertEquals("system1.stream1", config.getChangelogStream("store1"));
    assertEquals("system2.stream2", config.getChangelogStream("store2"));

    Map<String, String> configMapErr = new HashMap<>();
    configMapErr.put("stores.store4.changelog", "stream4"); // incorrect
    JavaStorageConfig configErr = new JavaStorageConfig(new MapConfig(configMapErr));

    try {
      configErr.getChangelogStream("store4");
      fail("store4 has no system defined. Should've failed.");
    } catch (Exception e) {
       // do nothing, it is expected
    }
  }

  @Test
  public void testEmptyStringOrNullChangelogStream() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("stores.store1.changelog", "");
    configMap.put("stores.store2.changelog", " ");

    JavaStorageConfig config = new JavaStorageConfig(new MapConfig(configMap));

    assertNull(config.getChangelogStream("store1"));
    assertNull(config.getChangelogStream("store2"));
    assertNull(config.getChangelogStream("store-changelog-none"));
  }
}
