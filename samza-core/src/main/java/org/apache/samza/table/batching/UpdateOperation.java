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

package org.apache.samza.table.batching;

import com.google.common.base.Preconditions;

/**
 * Update operation.
 *
 * @param <K> The type of the key.
 * @param <U> The type of the update
 */
public class UpdateOperation<K, V, U> implements Operation<K, V, U> {
  final private K key;
  final private U update;

  public UpdateOperation(K key, U update) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(update);
    this.key = key;
    this.update = update;
  }

  /**
   * @return The key to be updated in the table.
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * @return null.
   */
  @Override
  public V getValue() {
    return null;
  }

  /**
   * @return The Update to be applied to the table for the key.
   */
  @Override
  public U getUpdate() {
    return update;
  }

  @Override
  public Object[] getArgs() {
    return null;
  }
}
