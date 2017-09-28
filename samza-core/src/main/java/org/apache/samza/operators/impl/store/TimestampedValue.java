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
 *
 */
package org.apache.samza.operators.impl.store;

/**
 * An immutable pair of a value, and its corresponding timestamp.
 *
 * <p> Iterators on {@link TimeSeriesStore}s always return {@link TimestampedValue}s
 *
 * @param <V> the type of the value
 */
public class TimestampedValue<V> {
  private final V value;
  private final Long timestamp;

  public TimestampedValue(V v, Long time) {
    value = v;
    timestamp = time;
  }

  public V getValue() {
    return value;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !getClass().equals(o.getClass())) return false;

    TimestampedValue<?> that = (TimestampedValue<?>) o;

    if (value != null ? !value.equals(that.value) : that.value != null) return false;
    return timestamp.equals(that.timestamp);
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + timestamp.hashCode();
    return result;
  }
}
