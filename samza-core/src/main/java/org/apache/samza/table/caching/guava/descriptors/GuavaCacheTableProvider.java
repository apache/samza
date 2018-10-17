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

package org.apache.samza.table.caching.guava.descriptors;

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.caching.guava.GuavaCacheTable;
import org.apache.samza.table.utils.descriptors.BaseTableProvider;
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
    table.init(this.context);
    guavaTables.add(table);
    return table;
  }

  @Override
  public void close() {
    guavaTables.forEach(t -> t.close());
  }
}
