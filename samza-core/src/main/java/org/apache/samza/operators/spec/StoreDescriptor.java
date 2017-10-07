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
package org.apache.samza.operators.spec;

import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.Serde;

import java.util.HashMap;
import java.util.Map;

/**
 * A descriptor for a store.
 */
public class StoreDescriptor {
  private final String storeName;
  private final String storeFactory;
  private final Serde keySerde;
  private final Serde msgSerde;
  private final String changelogStream;
  private final Map<String, String> otherProperties;

  StoreDescriptor(String storeName, String storeFactory, Serde keySerde, Serde msgSerde,
      String changelogStream, Map<String, String> otherProperties) {
    this.storeName = storeName;
    this.storeFactory = storeFactory;
    this.keySerde = keySerde;
    this.msgSerde = msgSerde;
    this.changelogStream = changelogStream;
    this.otherProperties = otherProperties;
  }

  public String getStoreName() {
    return storeName;
  }

  public Serde getKeySerde() {
    return keySerde;
  }

  public Serde getMsgSerde() {
    return msgSerde;
  }

  public JavaStorageConfig getStorageConfigs() {
    HashMap<String, String> configs = new HashMap<>();
    configs.put(String.format(StorageConfig.FACTORY(), this.getStoreName()), this.getStoreFactory());
    configs.put(String.format(StorageConfig.CHANGELOG_STREAM(), this.getStoreName()), this.getChangelogStream());
    configs.putAll(this.getOtherProperties());
    return new JavaStorageConfig(new MapConfig(configs));
  }

  private String getStoreFactory() {
    return storeFactory;
  }

  private String getChangelogStream() {
    return changelogStream;
  }

  private Map<String, String> getOtherProperties() {
    return otherProperties;
  }
}