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

package org.apache.samza.operators;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.TableSpec;


/**
 * Base class for all table descriptor implementations.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
abstract public class BaseTableDescriptor<K, V, D extends BaseTableDescriptor<K, V, D>>
    implements TableDescriptor<K, V, D> {

  protected final String tableId;

  protected KVSerde<K, V> serde = KVSerde.of(new NoOpSerde(), new NoOpSerde());

  protected final Map<String, String> config = new HashMap<>();

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   */
  protected BaseTableDescriptor(String tableId) {
    this.tableId = tableId;
  }

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   * @param serde the serde for key and value
   */
  protected BaseTableDescriptor(String tableId, KVSerde<K, V> serde) {
    this.tableId = tableId;
    this.serde = serde;
  }

  @Override
  public D withConfig(String key, String value) {
    config.put(key, value);
    return (D) this;
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  /**
   * Get the serde assigned to this {@link TableDescriptor}
   *
   * @return {@link KVSerde} used by this table
   */
  public KVSerde<K, V> getSerde() {
    return serde;
  }

  /**
   * Generate config for {@link TableSpec}; this method is used internally.
   * @param tableSpecConfig configuration for the {@link TableSpec}
   */
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {
    tableSpecConfig.putAll(config);
  }

  /**
   * Validate that this table descriptor is constructed properly; this method is used internally.
   */
  protected void validate() {
  }

  /**
   * Create a {@link TableSpec} from this table descriptor; this method is used internally.
   *
   * @return the {@link TableSpec}
   */
  abstract public TableSpec getTableSpec();
}
