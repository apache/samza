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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.BaseTableProvider;
import org.apache.samza.table.descriptors.GuavaCacheTableDescriptor;
import org.apache.samza.table.utils.SerdeUtils;

import com.google.common.cache.Cache;


/**
 * Table provider for {@link GuavaCacheTable}.
 */
public class GuavaCacheTableProvider extends BaseTableProvider {

  private List<GuavaCacheTable> guavaTables = new ArrayList<>();

  public GuavaCacheTableProvider(String tableId) {
    super(tableId);
  }

  @Override
  public ReadWriteUpdateTable getTable() {
    Preconditions.checkNotNull(context, String.format("Table %s not initialized", tableId));
    JavaTableConfig tableConfig = new JavaTableConfig(context.getJobContext().getConfig());
    Cache guavaCache = SerdeUtils.deserialize(GuavaCacheTableDescriptor.GUAVA_CACHE,
        tableConfig.getForTable(tableId, GuavaCacheTableDescriptor.GUAVA_CACHE));
    GuavaCacheTable table = new GuavaCacheTable(tableId, guavaCache);
    table.init(this.context);
    guavaTables.add(table);
    return table;
  }

  @Override
  public void close() {
    super.close();
    guavaTables.forEach(t -> t.close());
  }
}
