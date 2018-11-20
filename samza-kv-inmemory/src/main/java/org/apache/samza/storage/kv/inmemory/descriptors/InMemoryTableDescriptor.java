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
package org.apache.samza.storage.kv.inmemory.descriptors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.kv.LocalTableProviderFactory;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory;
import org.apache.samza.table.descriptors.LocalTableDescriptor;

/**
 * Table descriptor for in-memory tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class InMemoryTableDescriptor<K, V> extends LocalTableDescriptor<K, V, InMemoryTableDescriptor<K, V>> {

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   * @param serde the serde for key and value
   */
  public InMemoryTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId, serde);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getProviderFactoryClassName() {
    return LocalTableProviderFactory.class.getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, String> toConfig(Config jobConfig) {
    Map<String, String> tableConfig = new HashMap<>(super.toConfig(jobConfig));
    // Store factory configuration
    tableConfig.put(String.format(StorageConfig.FACTORY(), tableId),
        InMemoryKeyValueStorageEngineFactory.class.getName());
    return Collections.unmodifiableMap(tableConfig);
  }
}
