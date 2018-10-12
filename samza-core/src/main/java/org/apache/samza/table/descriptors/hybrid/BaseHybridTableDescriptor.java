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
package org.apache.samza.table.descriptors.hybrid;

import java.util.List;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;

/**
 * Base class for hybrid table descriptors. A hybrid table consists of one or more
 * table descriptors, and it orchestrates operations between them to achieve more advanced
 * functionality.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @param <D> the type of this table descriptor
 */
abstract public class BaseHybridTableDescriptor<K, V, D extends BaseHybridTableDescriptor<K, V, D>>
    extends BaseTableDescriptor<K, V, D> {

  /**
   * {@inheritDoc}
   */
  public BaseHybridTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Get tables contained within this table.
   * @return list of tables
   */
  abstract public List<? extends TableDescriptor<K, V, ?>> getTableDescriptors();

}
