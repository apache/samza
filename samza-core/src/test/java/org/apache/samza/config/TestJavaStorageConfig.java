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
    map.put("stores.test.changelog", "testChangelog");
    map.put("stores.test.key.serde", "string");
    map.put("stores.test.msg.serde", "integer");
    JavaStorageConfig config = new JavaStorageConfig(new MapConfig(map));

    assertEquals("testFactory", config.getStorageFactoryClassName("test"));
    assertEquals("testChangelog", config.getChangelogStream("test"));
    assertEquals("string", config.getStorageKeySerde("test"));
    assertEquals("integer", config.getStorageMsgSerde("test"));
    assertEquals("test", config.getStoreNames().get(0));
  }
}
