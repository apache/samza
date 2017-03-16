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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLocalizerResourceMapper {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testResourceMapSuccess() throws Exception {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.localizer.resource.public.file.readme", "http://host1.com/readme");
    configMap.put("yarn.localizer.resource.private.archive.__package", "https://host2.com/package");
    configMap.put("yarn.localizer.resource.application.file.csr", "https://host3.com/csr");
    configMap.put("otherconfig", "https://host4.com/not_included");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

    LocalizerResourceMapper mapper = new LocalizerResourceMapper(conf, yarnConfiguration);
    mapper.map();
    Map<String, LocalResource> resourceMap = mapper.getResourceMap();

    assertEquals("resourceMap has 3 resources", 3, resourceMap.size());
  }

  @Test
  public void testResourceMapWithFileStatusFailure() throws Exception {
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("IO Exception when accessing the resource file status from the filesystem");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.localizer.resource.public.file.readme", "unknown://host1.com/readme");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    LocalizerResourceMapper mapper = new LocalizerResourceMapper(conf, yarnConfiguration);
    mapper.map();
  }

  @Test
  public void testResourceMapWithLocalizerResourceFailure() throws Exception {
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("Invalid resource visibility or type from localizer resource config key");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("yarn.localizer.resource.invalidVisibility.file.readme", "http://host1.com/readme");
    Config conf = new MapConfig(configMap);

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.http.impl", HttpFileSystem.class.getName());
    yarnConfiguration.set("fs.https.impl", HttpFileSystem.class.getName());
    LocalizerResourceMapper mapper = new LocalizerResourceMapper(conf, yarnConfiguration);
    mapper.map();
  }
}
