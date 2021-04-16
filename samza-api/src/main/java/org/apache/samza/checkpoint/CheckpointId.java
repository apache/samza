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
package org.apache.samza.checkpoint;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.annotation.InterfaceStability;

/**
 * Checkpoint ID has the format: [currentTimeMillis, last 6 digits of nanotime], separated by a dash.
 * This is to avoid conflicts, e.g when requesting frequent manual commits.
 *
 * It is expected that persistent stores use the {@link #toString()} representation of the checkpoint id
 * as the store checkpoint directory name.
 */
@InterfaceStability.Unstable
public class CheckpointId {
  public static final String SEPARATOR = "-";

  private final long millis;
  private final long nanos;

  private CheckpointId(long millis, long nanos) {
    this.millis = millis;
    this.nanos = nanos;
  }

  public static CheckpointId create() {
    return new CheckpointId(System.currentTimeMillis(), System.nanoTime() % 1000000);
  }

  public static CheckpointId fromString(String checkpointId) {
    if (StringUtils.isBlank(checkpointId)) {
      throw new IllegalArgumentException("Invalid checkpoint id: " + checkpointId);
    }
    String[] parts = checkpointId.split(SEPARATOR);
    return new CheckpointId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
  }

  public long getMillis() {
    return millis;
  }

  public long getNanos() {
    return nanos;
  }

  /**
   * WARNING: Do not change the toString() representation. It is used for serde'ing {@link CheckpointId} as part of task
   * checkpoints, in conjunction with {@link #fromString(String)}.
   * @return the String representation of this {@link CheckpointId}.
   */
  @Override
  public String toString() {
    return String.format("%s%s%s", millis, SEPARATOR, nanos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CheckpointId that = (CheckpointId) o;
    return millis == that.millis &&
        nanos == that.nanos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(millis, nanos);
  }
}