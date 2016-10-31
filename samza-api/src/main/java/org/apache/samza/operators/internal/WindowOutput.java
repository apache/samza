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
package org.apache.samza.operators.internal;

import org.apache.samza.operators.data.Message;


/**
 * This class defines the specific type of output messages from a {@link Operators.WindowOperator} function
 *
 * @param <K>  the type of key in the output window result
 * @param <M>  the type of value in the output window result
 */
public final class WindowOutput<K, M> implements Message<K, M> {
  private final K key;
  private final M value;

  WindowOutput(K key, M aggregated) {
    this.key = key;
    this.value = aggregated;
  }

  @Override public M getMessage() {
    return this.value;
  }

  @Override public K getKey() {
    return this.key;
  }

  @Override public long getTimestamp() {
    return 0;
  }

  static public <K, M> WindowOutput<K, M> of(K key, M result) {
    return new WindowOutput<>(key, result);
  }
}

