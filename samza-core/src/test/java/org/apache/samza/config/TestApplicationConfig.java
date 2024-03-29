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
import org.apache.samza.application.ApplicationApiType;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestApplicationConfig {
  @Test
  public void isHighLevelJob() {
    final Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.HIGH_LEVEL.name());
    ApplicationConfig applicationConfig = new ApplicationConfig(new MapConfig(configMap));

    assertTrue(applicationConfig.isHighLevelApiJob());
  }

  @Test
  public void isHighLevelJobWithLowLevelJob() {
    final Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LOW_LEVEL.name());
    ApplicationConfig applicationConfig = new ApplicationConfig(new MapConfig(configMap));

    assertFalse(applicationConfig.isHighLevelApiJob());
  }

  @Test
  public void isHighLevelJobWithLegacyJob() {
    final Map<String, String> configMap = new HashMap<>();
    configMap.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LEGACY.name());
    ApplicationConfig applicationConfig = new ApplicationConfig(new MapConfig(configMap));

    assertFalse(applicationConfig.isHighLevelApiJob());
  }
}
