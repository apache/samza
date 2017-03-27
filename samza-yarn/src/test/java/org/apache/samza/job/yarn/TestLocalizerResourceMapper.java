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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestLocalizerResourceMapper {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testResourceMapSuccess() {

    Map<String, String> configMap = new HashMap<>();

    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "file");
    configMap.put("yarn.resources.myResource1.local.visibility", "public");

    configMap.put("yarn.resources.myResource2.path", "https://host2.com/package");
    configMap.put("yarn.resources.myResource2.local.name", "__package");
    configMap.put("yarn.resources.myResource2.local.type", "archive");
    configMap.put("yarn.resources.myResource2.local.visibility", "private");

    configMap.put("yarn.resources.myResource3.path", "https://host3.com/csr");
    configMap.put("yarn.resources.myResource3.local.name", "csr");
    configMap.put("yarn.resources.myResource3.local.type", "file");
    configMap.put("yarn.resources.myResource3.local.visibility", "application");

    configMap.put("otherconfig", "https://host4.com/not_included");
    configMap.put("yarn.resources.myResource4.local.name", "notExisting");
    configMap.put("yarn.resources.myResource4.local.type", "file");
    configMap.put("yarn.resources.myResource4.local.visibility", "application");

    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());

    LocalizerResourceMapper mapper = new LocalizerResourceMapper(new LocalizerResourceConfig(conf), yarnConfiguration);
    Map<String, LocalResource> resourceMap = mapper.getResourceMap();

    assertEquals("resourceMap has 3 resources", 3, resourceMap.size());

    // resource1
    assertEquals("host1.com", resourceMap.get("readme").getResource().getHost());
    assertEquals(LocalResourceType.FILE, resourceMap.get("readme").getType());
    assertEquals(LocalResourceVisibility.PUBLIC, resourceMap.get("readme").getVisibility());

    // resource 2
    assertEquals("host2.com", resourceMap.get("__package").getResource().getHost());
    assertEquals(LocalResourceType.ARCHIVE, resourceMap.get("__package").getType());
    assertEquals(LocalResourceVisibility.PRIVATE, resourceMap.get("__package").getVisibility());

    // resource 3
    assertEquals("host3.com", resourceMap.get("csr").getResource().getHost());
    assertEquals(LocalResourceType.FILE, resourceMap.get("csr").getType());
    assertEquals(LocalResourceVisibility.APPLICATION, resourceMap.get("csr").getVisibility());

    // resource 4 should not exist
    assertNull("Resource does not exist with the name myResource4", resourceMap.get("myResource4"));
    assertNull("Resource does not exist with the defined config name notExisting for myResource4 either", resourceMap.get("notExisting"));
  }

  @Test
  public void testResourceMapWithDefaultValues() {

    Map<String, String> configMap = new HashMap<>();

    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");

    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());

    LocalizerResourceMapper mapper = new LocalizerResourceMapper(new LocalizerResourceConfig(conf), yarnConfiguration);
    Map<String, LocalResource> resourceMap = mapper.getResourceMap();

    assertNull("Resource does not exist with a name readme", resourceMap.get("readme"));
    assertNotNull("Resource exists with a name myResource1", resourceMap.get("myResource1"));
    assertEquals("host1.com", resourceMap.get("myResource1").getResource().getHost());
    assertEquals(LocalResourceType.FILE, resourceMap.get("myResource1").getType());
    assertEquals(LocalResourceVisibility.APPLICATION, resourceMap.get("myResource1").getVisibility());
  }

  @Test
  public void testResourceMapWithFileStatusFailure() {
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("IO Exception when accessing the resource file status from the filesystem");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "unknown://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "file");
    configMap.put("yarn.resources.myResource1.local.visibility", "public");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    LocalizerResourceMapper mapper = new LocalizerResourceMapper(new LocalizerResourceConfig(conf), yarnConfiguration);
  }

  @Test
  public void testResourceMapWithInvalidVisibilityFailure() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No enum constant org.apache.hadoop.yarn.api.records.LocalResourceVisibility.INVALIDVISIBILITY");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "file");
    configMap.put("yarn.resources.myResource1.local.visibility", "invalidVisibility");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    LocalizerResourceMapper mapper = new LocalizerResourceMapper(new LocalizerResourceConfig(conf), yarnConfiguration);
  }

  @Test
  public void testResourceMapWithInvalidTypeFailure() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No enum constant org.apache.hadoop.yarn.api.records.LocalResourceType.INVALIDTYPE");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.resources.myResource1.path", "http://host1.com/readme");
    configMap.put("yarn.resources.myResource1.local.name", "readme");
    configMap.put("yarn.resources.myResource1.local.type", "invalidType");
    configMap.put("yarn.resources.myResource1.local.visibility", "public");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    LocalizerResourceMapper mapper = new LocalizerResourceMapper(new LocalizerResourceConfig(conf), yarnConfiguration);
  }

}
