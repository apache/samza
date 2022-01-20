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
package org.apache.samza.storage.kv;

import com.google.common.base.Preconditions;
import org.apache.samza.context.Context;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.BaseTableProvider;

/**
 * Base class for tables backed by Samza local stores. The backing stores are
 * injected during initialization of the table. Since the lifecycle
 * of the underlying stores are already managed by Samza container,
 * the table provider will not manage the lifecycle of the backing
 * stores.
 */
public class LocalTableProvider extends BaseTableProvider {

  protected KeyValueStore kvStore;

  public LocalTableProvider(String tableId) {
    super(tableId);
  }

  @Override
  public void init(Context context) {
    super.init(context);

    Preconditions.checkNotNull(this.context, "Must specify context for local tables.");

    kvStore = this.context.getTaskContext().getStore(tableId);
    Preconditions.checkNotNull(kvStore, String.format(
        "Backing store for table %s was not injected by SamzaContainer", tableId));

    logger.info("Initialized backing store for table " + tableId);
  }

  @Override
  public ReadWriteUpdateTable getTable() {
    Preconditions.checkNotNull(context, String.format("Table %s not initialized", tableId));
    Preconditions.checkNotNull(kvStore, "Store not initialized for table " + tableId);
    @SuppressWarnings("unchecked")
    ReadWriteUpdateTable table = new LocalTable(tableId, kvStore);
    table.init(this.context);
    return table;
  }
}
