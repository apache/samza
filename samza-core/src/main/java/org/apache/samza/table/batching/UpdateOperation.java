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
import org.apache.samza.operators.UpdatePair;


/**
 * Update operation.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the default value
 * @param <U> The type of the update
 */
public class UpdateOperation<K, V, U> implements Operation<K, UpdatePair<U, V>> {
  final private K key;
  final private UpdatePair<U, V> val;
  final private Object[] args;

  public UpdateOperation(K key, UpdatePair<U, V> val, Object ... args) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(val);
    Preconditions.checkNotNull(args);
    this.key = key;
    this.val = val;
    this.args = args;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public UpdatePair<U, V> getValue() {
    return val;
  }

  @Override
  public Object[] getArgs() {
    return args;
  }
}
