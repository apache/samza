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

/**
 * a java version of the storage config
 */
public class JavaStorageConfig extends MapConfig {

  private static final String FACTORY_SUFFIX = ".factory";
  private static final String STORE_PREFIX = "stores.";
  private static final String FACTORY = "stores.%s.factory";
  private static final String KEY_SERDE = "stores.%s.key.serde";
  private static final String MSG_SERDE = "stores.%s.msg.serde";
  private static final String CHANGELOG_STREAM = "stores.%s.changelog";

  public JavaStorageConfig(Config config) {
    super(config);
  }

  public List<String> getStoreNames() {
    Config subConfig = subset(STORE_PREFIX, true);
    List<String> storeNames = new ArrayList<String>();
    for (String key : subConfig.keySet()) {
      if (key.endsWith(FACTORY_SUFFIX)) {
        storeNames.add(key.substring(0, key.length() - FACTORY_SUFFIX.length()));
      }
    }
    return storeNames;
  }

  public String getChangelogStream(String storeName) {
    return get(String.format(CHANGELOG_STREAM, storeName), null);
  }

  public String getStorageFactoryClassName(String storeName) {
    return get(String.format(FACTORY, storeName), null);
  }

  public String getStorageKeySerde(String storeName) {
    return get(String.format(KEY_SERDE, storeName), null);
  }

  public String getStorageMsgSerde(String storeName) {
    return get(String.format(MSG_SERDE, storeName), null);
  }
}