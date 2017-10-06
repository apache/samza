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

import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;

/**
 *
 * User facing class that collects metadata that fully describes a
 * Samza table. This class should be subclassed by the implementer of a
 * concrete table implementation.
 *
 * Once constructed, a table descriptor can be register to the system. Internally,
 * the table descriptor is then converted to a {@link TableSpec}, which is used to track
 * tables internally.
 *
 * Typical user code should look like the following, notice <code>withConfig()</code>
 * is defined in this class and the rest in subclasses.
 *
 * <pre>
 *   TableDescriptor tableDesc = new RocksDbTableFactory().getTableDescriptor("t1")
 *     .withConfig("some-key", "some-value")
 *     .withKeySerde(new IntegerSerde())
 *     .withValueSerde(new StringSerde("UTF-8"))
 *     .withBlockSize(1);
 * </pre>
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
@InterfaceStability.Unstable
abstract public class TableDescriptor<K, V, D extends TableDescriptor<K, V, D>> {

  protected final String tableId;

  protected Serde<K> keySerde = new NoOpSerde();
  protected Serde<V> valueSerde = new NoOpSerde();

  protected final Map<String, String> config = new HashMap<>();

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table
   */
  public TableDescriptor(String tableId) {
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
   * Set the Serde for keys of this table
   * @param keySerde the key serde
   * @return this table descriptor instance
   */
  public D withKeySerde(Serde<K> keySerde) {
    this.keySerde = keySerde;
    return (D) this;
  }

  /**
   * Set the Serde for values of this table
   * @param valueSerde the value serde
   * @return this table descriptor instance
   */
  public D withValueSerde(Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
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
   * Create a table spec from this table descriptor
   *
   * @return the table spec
   */
  abstract public TableSpec getTableSpec();

  /**
   * Generate config for {@link TableSpec}
   * @param tableSpecConfig configuration for the {@link TableSpec}
   */
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {
    tableSpecConfig.putAll(config);
  }

  /**
   * Validate that this table descriptor is constructed properly
   */
  protected void validate() {
    if (keySerde == null) {
      throw new SamzaException("Key serde not provided");
    }

    if (valueSerde == null) {
      throw new SamzaException("Value serde not provided");
    }
  }

  /**
   * Factory of a table descriptor object
   *
   * @param <D> the concrete type of the table descriptor. This is needed
   *            to be able to return the concrete type from the superclass,
   *            so that the API can be made fluent.
   */

  public interface Factory<D extends TableDescriptor> {
    /**
     * Create an instance of the underlying table descriptor
     * @param tableId the Id of the table instance, it is used to refer to
     *                the table throughout the job
     * @return the table descriptor instance
     */
    D getTableDescriptor(String tableId);
  }

}
