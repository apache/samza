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
 * Put operation.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value
 */
public class PutOperation<K, V> implements Operation<K, V> {
  final private K key;
  final private V val;

  public PutOperation(K key, V val) {
    Preconditions.checkNotNull(key);

    this.key = key;
    this.val = val;
  }

  /**
   * @return The key to be put to the table.
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * @return The value to be put to the table.
   */
  @Override
  public V getValue() {
    return val;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    final PutOperation<K, V> otherOp = (PutOperation) other;

    // If value for this object is null, should be null for other object as well.
    if ((getValue() == null) ^ (otherOp.getValue() == null)) {
      return false;
    }

    return getKey().equals(otherOp.getKey()) && getValue().equals(otherOp.getValue());
  }

  @Override
  public int hashCode() {
    return getKey().hashCode();
  }
}
