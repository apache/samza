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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestFsImplConfigManager {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFsImplConfigManagerSuccess() throws Exception {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("fs.http.impl", "org.apache.samza.HttpFileSystem");
    configMap.put("fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem");

    Config conf = new MapConfig(configMap);

    FsImplConfigManager manager = new FsImplConfigManager(conf);
    assertEquals(2, manager.getSchemes().size());
    assertEquals("http", manager.getSchemes().get(0));
    assertEquals("myscheme", manager.getSchemes().get(1));

    assertEquals("fs.http.impl", manager.getFsImplKey("http"));
    assertEquals("fs.myscheme.impl", manager.getFsImplKey("myscheme"));

    assertEquals("org.apache.samza.HttpFileSystem", manager.getFsImplClassName("http"));
    assertEquals("org.apache.samza.MySchemeFileSystem", manager.getFsImplClassName("myscheme"));
  }

  @Test
  public void testNullConfig() throws Exception {
    FsImplConfigManager manager = new FsImplConfigManager(null);
    assertEquals(new ArrayList<String>(), manager.getSchemes());
  }

  @Test
  public void testEmptyImpl() throws Exception {
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("fs.http.impl does not have configured class implementation");

    Map<String, String> configMap = new HashMap<>();
    configMap.put("fs.http.impl", "");
    Config conf = new MapConfig(configMap);

    FsImplConfigManager manager = new FsImplConfigManager(conf);
    manager.getFsImplClassName("http");
  }

  @Test
  public void testOverrideYarnConfiguration() throws Exception {
    // initial yarn config
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.myscheme.impl", "org.hadoop.MySchemeFileSystem");

    //samza job config
    Map<String, String> configMap = new HashMap<>();
    configMap.put("fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem");
    Config conf = new MapConfig(configMap);

    FsImplConfigManager manager = new FsImplConfigManager(conf);
    manager.overrideYarnConfiguration(yarnConfiguration);

    assertEquals("org.apache.samza.MySchemeFileSystem", yarnConfiguration.get("fs.myscheme.impl"));
  }

  @Test
  public void testOverrideNullYarnConfiguration() throws Exception {
    // initial yarn config
    YarnConfiguration yarnConfiguration = null;

    //samza job config
    Map<String, String> configMap = new HashMap<>();
    configMap.put("fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem");
    Config conf = new MapConfig(configMap);

    FsImplConfigManager manager = new FsImplConfigManager(conf);
    manager.overrideYarnConfiguration(yarnConfiguration);

    assertNull(yarnConfiguration);
  }

  @Test
  public void testOverrideYarnConfigurationWithEmptyImpl() throws Exception {
    // initial yarn config
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set("fs.myscheme.impl", "org.hadoop.MySchemeFileSystem");

    //samza job config
    Map<String, String> configMap = new HashMap<>();
    configMap.put("fs.myscheme.impl", "");
    Config conf = new MapConfig(configMap);

    FsImplConfigManager manager = new FsImplConfigManager(conf);
    manager.overrideYarnConfiguration(yarnConfiguration);

    assertEquals("org.hadoop.MySchemeFileSystem", yarnConfiguration.get("fs.myscheme.impl"));
  }

}
