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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.SideInputProcessor;


/**
 * TableSpec is a blueprint for creating, validating, or simply describing a table in the runtime environment.
 *
 * It is typically created indirectly by constructing an instance of {@link org.apache.samza.operators.TableDescriptor},
 * and then invoke <code>BaseTableDescriptor.getTableSpec()</code>.
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
public class TableSpec implements Serializable {

  private final String id;
  private final String tableProviderFactoryClassName;

  /**
   * The following fields are serialized by the ExecutionPlanner when generating the configs for a table, and deserialized
   * once during startup in SamzaContainer. They don't need to be deserialized here on a per-task basis
   */
  private transient final KVSerde serde;
  private transient final List<String> sideInputs = new ArrayList<>();
  private transient final SideInputProcessor sideInputProcessor;
  private transient final Map<String, String> config = new HashMap<>();

  /**
   * Default constructor
   */
  public TableSpec() {
    this.id = null;
    this.serde = null;
    this.tableProviderFactoryClassName = null;
    this.sideInputProcessor = null;
  }

  /**
   * Constructs a {@link TableSpec}
   *
   * @param tableId Id of the table
   * @param tableProviderFactoryClassName table provider factory
   * @param serde the serde
   * @param config implementation specific configuration
   */
  public TableSpec(String tableId, KVSerde serde, String tableProviderFactoryClassName, Map<String, String> config) {
    this(tableId, serde, tableProviderFactoryClassName, config, Collections.emptyList(), null);
  }

  /**
   * Constructs a {@link TableSpec}
   *
   * @param tableId Id of the table
   * @param tableProviderFactoryClassName table provider factory
   * @param serde the serde
   * @param config implementation specific configuration
   * @param sideInputs list of side inputs for the table
   * @param sideInputProcessor side input processor for the table
   */
  public TableSpec(String tableId, KVSerde serde, String tableProviderFactoryClassName, Map<String, String> config,
      List<String> sideInputs, SideInputProcessor sideInputProcessor) {
    this.id = tableId;
    this.serde = serde;
    this.tableProviderFactoryClassName = tableProviderFactoryClassName;
    this.config.putAll(config);
    this.sideInputs.addAll(sideInputs);
    this.sideInputProcessor = sideInputProcessor;
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
  public String getTableProviderFactoryClassName() {
    return tableProviderFactoryClassName;
  }

  /**
   * Get implementation configuration for the table
   * @return configuration for the table
   */
  public Map<String, String> getConfig() {
    return Collections.unmodifiableMap(config);
  }

  /**
   * Get the list of side inputs for the table.
   *
   * @return a {@link List} of side input streams
   */
  public List<String> getSideInputs() {
    return sideInputs;
  }

  /**
   * Get the {@link SideInputProcessor} associated with the table.
   *
   * @return a {@link SideInputProcessor}
   */
  public SideInputProcessor getSideInputProcessor() {
    return sideInputProcessor;
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
