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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.BaseTableProvider;
import org.apache.samza.table.utils.SerdeUtils;

import com.google.common.cache.Cache;


/**
 * Table provider for {@link GuavaCacheTable}.
 */
public class GuavaCacheTableProvider extends BaseTableProvider {

  public static final String GUAVA_CACHE = "guavaCache";

  private List<GuavaCacheTable> guavaTables = new ArrayList<>();

  public GuavaCacheTableProvider(TableSpec tableSpec) {
    super(tableSpec);
  }

  @Override
  public Table getTable() {
    Cache guavaCache = SerdeUtils.deserialize(GUAVA_CACHE, tableSpec.getConfig().get(GUAVA_CACHE));
    GuavaCacheTable table = new GuavaCacheTable(tableSpec.getId(), guavaCache);
    table.init(containerContext, taskContext);
    guavaTables.add(table);
    return table;
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {
    Map<String, String> tableConfig = new HashMap<>();

    // Insert table_id prefix to config entries
    tableSpec.getConfig().forEach((k, v) -> {
        String realKey = String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
        tableConfig.put(realKey, v);
      });

    logger.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }

  @Override
  public void close() {
    guavaTables.forEach(t -> t.close());
  }
}
