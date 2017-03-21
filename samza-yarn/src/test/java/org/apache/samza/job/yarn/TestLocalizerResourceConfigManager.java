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
package org.apache.samza.job.yarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLocalizerResourceConfigManager {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testResourceConfigIncluded() throws Exception {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "file");
    configMap.put("yarn.resources.myResource1.local.visibility", "public");

    Config conf = new MapConfig(configMap);

    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(conf);
    assertEquals(1, manager.getResourceNames().size());
    assertEquals("myResource1", manager.getResourceNames().get(0));
    assertEquals("readme", manager.getResourceLocalName("myResource1"));
    assertEquals(LocalResourceType.FILE, manager.getResourceLocalType("myResource1"));
    assertEquals(LocalResourceVisibility.PUBLIC, manager.getResourceLocalVisibility("myResource1"));
  }

  @Test
  public void testResourcrConfigNotIncluded() throws Exception {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("otherconfig", "https://host2.com/not_included");
    configMap.put("yarn.resources.myResource2.local.name", "notExisting");
    configMap.put("yarn.resources.myResource2.local.type", "file");
    configMap.put("yarn.resources.myResource2.local.visibility", "application");

    Config conf = new MapConfig(configMap);

    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(conf);
    assertEquals(0, manager.getResourceNames().size());
  }

  @Test
  public void testNullConfig() throws Exception {
    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(null);
    assertEquals(new ArrayList<String>(), manager.getResourceNames());
  }

  @Test
  public void testInvalidVisibility() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No enum constant org.apache.hadoop.yarn.api.records.LocalResourceVisibility.INVALIDVISIBILITY");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "file");
    configMap.put("yarn.resources.myResource1.local.visibility", "invalidVisibility");
    Config conf = new MapConfig(configMap);

    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(conf);
    manager.getResourceLocalVisibility("myResource1");
  }

  @Test
  public void testInvalidType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No enum constant org.apache.hadoop.yarn.api.records.LocalResourceType.INVALIDTYPE");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "invalidType");
    configMap.put("yarn.resources.myResource1.local.visibility", "application");
    Config conf = new MapConfig(configMap);

    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(conf);
    manager.getResourceLocalType("myResource1");
  }

  @Test
  public void testInvalidPath() throws Exception {
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("resource path is required but not defined in config for resource myResource1");
    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "invalidType");
    configMap.put("yarn.resources.myResource1.local.visibility", "application");
    Config conf = new MapConfig(configMap);

    LocalizerResourceConfigManager manager = new LocalizerResourceConfigManager(conf);
    manager.getResourcePath("myResource1");
  }

}
