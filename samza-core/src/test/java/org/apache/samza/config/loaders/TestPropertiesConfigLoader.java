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

package org.apache.samza.config.loaders;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestPropertiesConfigLoader {
  @Test
  public void testCanReadPropertiesConfigFiles() {
    ConfigLoader loader = new PropertiesConfigLoader(
        new MapConfig(Collections.singletonMap(PropertiesConfigLoader.PATH, getClass().getResource("/test.properties").getPath())));

    Config config = loader.getConfig();
    assertEquals("bar", config.get("foo"));
  }

  @Test
  public void testOverrideLoaderValueWithProvidedOverrides() {
    String jobName = "overridden-job-name";
    ConfigLoader loader = new PropertiesConfigLoader(
        new MapConfig(ImmutableMap.of(
            JobConfig.JOB_NAME, jobName,
            PropertiesConfigLoader.PATH, getClass().getResource("/test.properties").getPath())));

    Config config = loader.getConfig();
    assertEquals("bar", config.get("foo"));
    assertEquals(jobName, config.get(JobConfig.JOB_NAME));
  }

  @Test
  public void testCanNotReadWithoutPath() {
    try {
      ConfigLoader loader = new PropertiesConfigLoader(new MapConfig());
      loader.getConfig();
      fail("should have gotten a samza exception");
    } catch (SamzaException e) {
      // Do nothing
    }
  }
}