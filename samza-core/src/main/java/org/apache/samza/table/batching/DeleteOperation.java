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
 * Delete operation.
 *
 * @param <K> The type of the key.
 */
public class DeleteOperation<K, V> implements Operation<K, V> {
  final K key;

  public DeleteOperation(K key) {
    Preconditions.checkNotNull(key);

    this.key = key;
  }

  /**
   * @return The key to be deleted.
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


  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    final DeleteOperation<K, V> otherOp = (DeleteOperation) other;
    return otherOp.getValue() == null && getKey().equals(otherOp.getKey());
  }

  @Override
  public int hashCode() {
    return getKey().hashCode();
  }
}
