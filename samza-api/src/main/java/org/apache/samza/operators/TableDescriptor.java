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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.TableSpec;

/**
 *
 * User facing class that collects metadata that fully describes a
 * Samza table. This class should be subclassed by the implementer of a
 * concrete table implementation.
 *
 * Once constructed, a table descriptor can be registered with the system. Internally,
 * the table descriptor is then converted to a {@link TableSpec}, which is used to track
 * tables internally.
 *
 * Typical user code should look like the following, notice <code>withConfig()</code>
 * is defined in this class and the rest in subclasses.
 *
 * <pre>
 *   TableDescriptor&lt;Integer, String, ?&gt; tableDesc = new RocksDbTableDescriptor("t1")
 *     .withKeySerde(new IntegerSerde())
 *     .withValueSerde(new StringSerde("UTF-8"))
 *     .withBlockSize(1)
 *     .withConfig("some-key", "some-value");
 * </pre>
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
@InterfaceStability.Unstable
abstract public class TableDescriptor<K, V, D extends TableDescriptor<K, V, D>> {

  protected final String tableId;

  protected KVSerde<K, V> serde = KVSerde.of(new NoOpSerde(), new NoOpSerde());

  protected final Map<String, String> config = new HashMap<>();

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table
   */
  protected TableDescriptor(String tableId) {
    this.tableId = tableId;
  }

  /**
   * Add a configuration entry for the table
   * @param key the key
   * @param value the value
   * @return this table descriptor instance
   */
  public D withConfig(String key, String value) {
    config.put(key, value);
    return (D) this;
  }

  /**
   * Set the Serde for this table
   * @param serde the serde
   * @return this table descriptor instance
   * @throws IllegalArgumentException if null is provided
   */
  public D withSerde(KVSerde<K, V> serde) {
    if (serde == null) {
      throw new IllegalArgumentException("Serde cannot be null");
    }
    this.serde = serde;
    return (D) this;
  }

  /**
   * Get the Id of the table
   * @return Id of the table
   */
  public String getTableId() {
    return tableId;
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
