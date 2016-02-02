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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * a java version of the system config
 */
public class JavaSystemConfig extends MapConfig {
  private static final String SYSTEM_PREFIX = "systems.";
  private static final String SYSTEM_FACTORY_SUFFIX = ".samza.factory";
  private static final String SYSTEM_FACTORY = "systems.%s.samza.factory";
  private static final String EMPTY = "";

  public JavaSystemConfig(Config config) {
    super(config);
  }

  public String getSystemFactory(String name) {
    if (name == null) {
      return null;
    }
    String systemFactory = String.format(SYSTEM_FACTORY, name);
    return get(systemFactory, null);
  }

  /**
   * Get a list of system names.
   *
   * @return A list system names
   */
  public List<String> getSystemNames() {
    Config subConf = subset(SYSTEM_PREFIX, true);
    ArrayList<String> systemNames = new ArrayList<String>();
    for (Map.Entry<String, String> entry : subConf.entrySet()) {
      String key = entry.getKey();
      if (key.endsWith(SYSTEM_FACTORY_SUFFIX)) {
        systemNames.add(key.replace(SYSTEM_FACTORY_SUFFIX, EMPTY));
      }
    }
    return systemNames;
  }
}
