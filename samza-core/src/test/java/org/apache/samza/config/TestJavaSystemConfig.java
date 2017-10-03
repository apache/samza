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

public class TestJavaSystemConfig {
  private static final String MOCK_SYSTEM_NAME1 = "mocksystem1";
  private static final String MOCK_SYSTEM_NAME2 = "mocksystem2";
  private static final String MOCK_SYSTEM_FACTORY_NAME1 = String.format(JavaSystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME1);
  private static final String MOCK_SYSTEM_FACTORY_NAME2 = String.format(JavaSystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME2);
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME1 = "some.factory.Class1";
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME2 = "some.factory.Class2";

  @Test
  public void testClassName() {
    Map<String, String> map = new HashMap<String, String>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, MOCK_SYSTEM_FACTORY_CLASSNAME1);
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertEquals(MOCK_SYSTEM_FACTORY_CLASSNAME1, systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1));
  }

  @Test
  public void testGetEmptyClassNameAsNull() {
    Map<String, String> map = new HashMap<String, String>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, "");
    map.put(MOCK_SYSTEM_FACTORY_NAME2, " ");
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertNull(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1));
    assertNull(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemNames() {
    Map<String, String> map = new HashMap<String, String>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, MOCK_SYSTEM_FACTORY_CLASSNAME1);
    map.put(MOCK_SYSTEM_FACTORY_NAME2, MOCK_SYSTEM_FACTORY_CLASSNAME2);
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertEquals(2, systemConfig.getSystemNames().size());
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME1));
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME2));
  }
}
