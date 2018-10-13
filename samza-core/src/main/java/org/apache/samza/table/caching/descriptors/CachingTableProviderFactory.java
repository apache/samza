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

package org.apache.samza.table.caching.descriptors;

import org.apache.samza.table.descriptors.TableProvider;
import org.apache.samza.table.descriptors.TableProviderFactory;
import org.apache.samza.table.TableSpec;

/**
 * Table provider factory for {@link org.apache.samza.table.caching.CachingTable}.
 */
public class CachingTableProviderFactory implements TableProviderFactory {
  @Override
  public TableProvider getTableProvider(TableSpec tableSpec) {
    return new CachingTableProvider(tableSpec);
  }
}
