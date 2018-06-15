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

package org.apache.samza.table.caching.guava;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.SerdeUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;


/**
 * Table descriptor for {@link GuavaCacheTable}.
 * @param <K> type of the key in the cache
 * @param <V> type of the value in the cache
 */
public class GuavaCacheTableDescriptor<K, V> extends BaseTableDescriptor<K, V, GuavaCacheTableDescriptor<K, V>> {
  private Cache<K, V> cache;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table
   */
  public GuavaCacheTableDescriptor(String tableId) {
    super(tableId);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    tableSpecConfig.put(GuavaCacheTableProvider.GUAVA_CACHE, SerdeUtils.serialize("Guava cache", cache));

    return new TableSpec(tableId, serde, GuavaCacheTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Specify a pre-configured Guava cache instance to be used for caching table.
   * @param cache Guava cache instance
   * @return this descriptor
   */
  public GuavaCacheTableDescriptor withCache(Cache<K, V> cache) {
    this.cache = cache;
    return this;
  }

  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkArgument(cache != null, "Must provide a Guava cache instance.");
  }
}
