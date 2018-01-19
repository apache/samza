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

package org.apache.samza.system;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;


public class SystemAdmins {
  private final Map<String, SystemAdmin> systemAdminMap;

  public Map<String, SystemAdmin> systemAdminMap() {
    return systemAdminMap;
  }

  public SystemAdmins(Config config) {
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
    this.systemAdminMap = systemConfig.getSystemAdmins();
  }

  public SystemAdmins() {
    this(new HashMap<>());
  }

  // Used only for test
  public SystemAdmins(Map<String, SystemAdmin> systemAdminMap) {
    this.systemAdminMap = systemAdminMap;
  }

  public void start() {
    for (SystemAdmin systemAdmin: systemAdminMap.values())
      systemAdmin.start();
  }

  public void stop() {
    for (SystemAdmin systemAdmin: systemAdminMap.values())
      systemAdmin.stop();
  }

  public SystemAdmin getSystemAdmin(String systemName) {
    if (!systemAdminMap.containsKey(systemName))
      throw new SamzaException("Cannot get systemAdmin for system " + systemName);
    return systemAdminMap.get(systemName);
  }

}
