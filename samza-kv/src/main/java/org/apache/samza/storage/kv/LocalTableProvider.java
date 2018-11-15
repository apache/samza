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
import java.util.regex.Pattern;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.BaseTableProvider;

/**
 * Base class for tables backed by Samza local stores. The backing stores are
 * injected during initialization of the table. Since the lifecycle
 * of the underlying stores are already managed by Samza container,
 * the table provider will not manage the lifecycle of the backing
 * stores.
 */
public class LocalTableProvider extends BaseTableProvider {
  public static final Pattern SYSTEM_STREAM_NAME_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  protected KeyValueStore kvStore;

  public LocalTableProvider(String tableId, Config config) {
    super(tableId, config);
  }

  @Override
  public void init(Context context) {
    super.init(context);

    Preconditions.checkNotNull(this.context, "Must specify context for local tables.");

    kvStore = (KeyValueStore) this.context.getTaskContext().getStore(tableId);

    if (kvStore == null) {
      throw new SamzaException(String.format(
          "Backing store for table %s was not injected by SamzaContainer", tableId));
    }

    logger.info("Initialized backing store for table " + tableId);
  }

  @Override
  public Table getTable() {
    if (kvStore == null) {
      throw new SamzaException("Store not initialized for table " + tableId);
    }
    ReadableTable table = new LocalReadWriteTable(tableId, kvStore);
    table.init(this.context);
    return table;
  }
}
