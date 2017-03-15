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

package org.apache.samza.operators.impl;

import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.windows.WindowKey;

/**
 * Uniquely identifies a trigger firing
 */
class TriggerKey<T> {
  private final FiringType type;
  private final WindowKey<T> key;

  public TriggerKey(FiringType type, WindowKey<T> key) {
    if (type == null) {
      throw new IllegalArgumentException("Firing type cannot be null");
    }

    if (key == null) {
      throw new IllegalArgumentException("WindowKey cannot be null");
    }

    this.type = type;
    this.key = key;
  }

  /**
   * Equality is determined by both the type, and the window key.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TriggerKey<T> that = (TriggerKey<T>) o;
    return type == that.type && key.equals(that.key);
  }

  /**
   * Hashcode is computed by from the type, and the window key.
   */
  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + key.hashCode();
    return result;
  }

  public WindowKey<T> getKey() {
    return key;
  }

  public FiringType getType() {
    return type;
  }
}