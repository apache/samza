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
package org.apache.samza.storage.kv.inmemory;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.kv.BaseLocalStoreBackedTableDescriptor;
import org.apache.samza.table.TableSpec;


/**
 * Table descriptor for in-memory tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class InMemoryTableDescriptor<K, V> extends BaseLocalStoreBackedTableDescriptor<K, V, InMemoryTableDescriptor<K, V>> {

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   */
  public InMemoryTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   * @param serde the serde for key and value
   */
  public InMemoryTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId, serde);
  }

  @Override
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {
    super.generateTableSpecConfig(tableSpecConfig);
  }

  @Override
  public TableSpec getTableSpec() {

    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    return new TableSpec(tableId, serde, InMemoryTableProviderFactory.class.getName(), tableSpecConfig,
        sideInputs, sideInputsProcessor);
  }

  private void addInMemoryConfig(Map<String, String> map, String key, String value) {
    map.put("inmemory." + key, value);
  }
}
