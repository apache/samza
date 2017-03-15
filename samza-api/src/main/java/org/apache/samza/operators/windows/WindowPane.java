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
package org.apache.samza.operators.windows;

import org.apache.samza.operators.triggers.FiringType;

/**
 * Specifies the result emitted from a {@link Window}.
 *
 * @param <K>  the type of key in the window pane
 * @param <V>  the type of value in the window pane.
 */
public final class WindowPane<K, V> {

  private final WindowKey<K> key;

  private final V value;

  private final AccumulationMode mode;

  /**
   * The type of the trigger that emitted this result. Results can be emitted from early, late or default triggers.
   */
  private final FiringType type;

  public WindowPane(WindowKey<K> key, V value, AccumulationMode mode, FiringType type) {
    this.key = key;
    this.value = value;
    this.mode = mode;
    this.type = type;
  }

  public V getMessage() {
    return this.value;
  }

  public WindowKey<K> getKey() {
    return this.key;
  }

  public FiringType getFiringType() {
    return type;
  }
}


