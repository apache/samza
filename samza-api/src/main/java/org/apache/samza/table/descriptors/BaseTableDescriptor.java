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

package org.apache.samza.table.descriptors;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;

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

  protected final Map<String, String> config = new HashMap<>();

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   */
  protected BaseTableDescriptor(String tableId) {
    this.tableId = tableId;
  }

  /**
   * Add a configuration entry for the table
   *
   * @param key the key
   * @param value the value
   * @return this table descriptor instance
   */
  @SuppressWarnings("unchecked")
  public D withConfig(String key, String value) {
    config.put(key, value);
    return (D) this;
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  @Override
  public Map<String, String> toConfig(Config jobConfig) {

    Preconditions.checkNotNull(jobConfig, "Job config is null");

    validate();

    Map<String, String> tableConfig = new HashMap<>(config);
    tableConfig.put(
        String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableId),
        getProviderFactoryClassName());

    return Collections.unmodifiableMap(tableConfig);
  }

  /**
   * Return the fully qualified class name of the {@link org.apache.samza.table.TableProviderFactory}
   * @return class name of the {@link org.apache.samza.table.TableProviderFactory}
   */
  abstract public String getProviderFactoryClassName();

  /**
   * Validate that this table descriptor is constructed properly; this method is used internally.
   */
  abstract protected void validate();

  /**
   * Helper method to add a config item to table configuration
   * @param key key of the config item
   * @param value value of the config item
   * @param tableConfig table configuration
   */
  protected void addTableConfig(String key, String value, Map<String, String> tableConfig) {
    tableConfig.put(JavaTableConfig.buildKey(tableId, key), value);
  }

}
