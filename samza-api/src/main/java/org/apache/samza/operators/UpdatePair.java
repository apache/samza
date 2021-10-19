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
package org.apache.samza.operators;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * A UpdatePair representing the update and an optional default record to be inserted for a key,
 * if the update is applied to a non-existent record.
 *
 * @param <U> type of the update record
 * @param <V> type of the default record
 */
public final class UpdatePair<U, V> {
  private final U update;
  @Nullable private final V defaultValue;

  public static <U, V> UpdatePair<U, V> of(U update, @Nullable V defaultValue) {
    return new UpdatePair<>(update, defaultValue);
  }

  public static <U, V> UpdatePair<U, V> of(U update) {
    return new UpdatePair<>(update, null);
  }

  private UpdatePair(U update, V defaultValue) {
    this.update = update;
    this.defaultValue = defaultValue;
  }

  public U getUpdate() {
    return update;
  }

  public V getDefault() {
    return defaultValue;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof UpdatePair)) {
      return false;
    }
    UpdatePair<?, ?> otherPair = (UpdatePair<?, ?>) other;
    return Objects.deepEquals(this.update, otherPair.getUpdate())
        && Objects.deepEquals(this.defaultValue, otherPair.getDefault());
  }

  @Override
  public int hashCode() {
    return Objects.hash(update, defaultValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(update)
        .addValue(defaultValue)
        .toString();
  }
}
