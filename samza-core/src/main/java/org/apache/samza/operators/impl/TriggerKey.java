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

/**
 * Uniquely identifies a trigger firing
 */
public class TriggerKey<K> {
  private final FiringType type;
  private final K key;
  private final long timestamp;

  public TriggerKey(FiringType type, K key, long timestamp) {
    if (type == null) {
      throw new IllegalArgumentException("Firing type cannot be null");
    }

    this.type = type;
    this.key = key;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TriggerKey<?> that = (TriggerKey<?>) o;

    if (timestamp != that.timestamp) {
      return false;
    }
    if (type != that.type) {
      return false;
    }
    return key != null ? key.equals(that.key) : that.key == null;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  public K getKey() {
    return key;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public FiringType getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("TriggerKey: {type=%s, key=%s}", type, key);
  }
}