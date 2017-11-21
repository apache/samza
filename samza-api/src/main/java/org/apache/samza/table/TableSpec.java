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
package org.apache.samza.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.KVSerde;


/**
 * TableSpec is a blueprint for creating, validating, or simply describing a table in the runtime environment.
 *
 * It is typically created indirectly by constructing an instance of {@link org.apache.samza.operators.TableDescriptor},
 * and then invoke <code>TableDescriptor.getTableSpec()</code>.
 *
 * It has specific attributes for common behaviors that Samza uses.
 *
 * It has the table provider factory, which provides the actual table implementation.
 *
 * It also includes a map of configurations which may be implementation-specific.
 *
 * It is immutable by design.
 */
@InterfaceStability.Unstable
public class TableSpec {

  private final String id;
  private final KVSerde serde;
  private final String tableProviderFactory;
  private final Map<String, String> config = new HashMap<>();

  /**
   * Default constructor
   */
  public TableSpec() {
    this.id = null;
    this.serde = null;
    this.tableProviderFactory = null;
  }

  /**
   * Constructs a {@link TableSpec}
   *
   * @param tableId Id of the table
   * @param tableProviderFactory table provider factory
   * @param serde the serde
   * @param config implementation specific configuration
   */
  public TableSpec(String tableId, KVSerde serde, String tableProviderFactory,
      Map<String, String> config) {
    this.id = tableId;
    this.serde = serde;
    this.tableProviderFactory = tableProviderFactory;
    this.config.putAll(config);
  }

  /**
   * Get the Id of the table
   * @return Id of the table
   */
  public String getId() {
    return id;
  }

  /**
   * Get the serde
   * @param <K> the type of the key
   * @param <V> the type of the value
   * @return the key serde
   */
  public <K, V> KVSerde<K, V> getSerde() {
    return serde;
  }

  /**
   * Get the class name of the table provider factory
   * @return class name of the table provider factory
   */
  public String getTableProviderFactory() {
    return tableProviderFactory;
  }

  /**
   * Get implementation configuration for the table
   * @return configuration for the table
   */
  public Map<String, String> getConfig() {
    return Collections.unmodifiableMap(config);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    return id.equals(((TableSpec) o).id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
