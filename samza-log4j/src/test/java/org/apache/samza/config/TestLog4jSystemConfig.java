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

import org.apache.samza.logging.log4j.serializers.LoggingEventStringSerdeFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestLog4jSystemConfig {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testGetSystemNamesAndGetValue() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("systems.system1.samza.factory","1");
    map.put("systems.system2.samza.factory","2");
    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));

    assertEquals(2, log4jSystemConfig.getSystemNames().size());
    assertEquals("1", log4jSystemConfig.getValue("systems.system1.samza.factory"));
  }

  @Test
  public void testGetLog4jSystemName() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("task.log4j.system", "log4j-system");
    map.put("systems.system1.samza.factory","1");

    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));
    assertEquals("log4j-system", log4jSystemConfig.getSystemName());

    // use the default system name
    map.remove("task.log4j.system");
    log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));
    assertEquals("system1", log4jSystemConfig.getSystemName());

    // throw ConfigException
    map.put("systems.system2.samza.factory", "2");
    log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));
    exception.expect(ConfigException.class);
    log4jSystemConfig.getSystemName();
  }

  @Test
  public void testGetSerdeClass() {
    Map<String, String> map = new HashMap<String, String>();
    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));

    // get the default serde
    assertEquals(LoggingEventStringSerdeFactory.class.getCanonicalName(), log4jSystemConfig.getSerdeClass("log4j"));
    // get null
    assertNull(log4jSystemConfig.getSerdeClass("otherName"));
  }

  @Test
  public void testGetSerdeName() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("systems.mockSystem.streams.mockStream.samza.msg.serde", "streamSerde");
    map.put("systems.mockSystem.samza.msg.serde", "systemSerde");
    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(new MapConfig(map));

    assertEquals("streamSerde", log4jSystemConfig.getStreamSerdeName("mockSystem", "mockStream"));
    assertEquals("systemSerde", log4jSystemConfig.getSystemSerdeName("mockSystem"));
  }
}
