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
 * It is expected that persistent stores use the {@link #serialize()} representation of the checkpoint id
 * as the store checkpoint directory name.
 */
@InterfaceStability.Unstable
public class CheckpointId implements Comparable<CheckpointId> {
  public static final String SEPARATOR = "-";

  private final long millis;
  private final long nanoId;

  private CheckpointId(long millis, long nanoId) {
    this.millis = millis;
    this.nanoId = nanoId;
  }

  public static CheckpointId create() {
    return new CheckpointId(System.currentTimeMillis(), System.nanoTime() % 1000000);
  }

  public static CheckpointId deserialize(String checkpointId) {
    if (StringUtils.isBlank(checkpointId)) {
      throw new IllegalArgumentException("Invalid checkpoint id: " + checkpointId);
    }
    try {
      String[] parts = checkpointId.split(SEPARATOR);
      return new CheckpointId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(String.format(
          "Could not deserialize CheckpointId: %s", checkpointId), ex);
    }
  }

  public long getMillis() {
    return millis;
  }

  public long getNanoId() {
    return nanoId;
  }

  /**
   * Serialization of {@link CheckpointId} as part of task checkpoints, in conjunction with {@link #deserialize(String)}.
   * @return the String representation of this {@link CheckpointId}.
   */
  public String serialize() {
    return String.format("%s%s%s", getMillis(), SEPARATOR, getNanoId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CheckpointId that = (CheckpointId) o;
    return millis == that.millis &&
        nanoId == that.nanoId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(millis, nanoId);
  }

  @Override
  public int compareTo(CheckpointId that) {
    if (this.millis != that.millis) return Long.compare(this.millis, that.millis);
    else return Long.compare(this.nanoId, that.nanoId);
  }

  @Override
  public String toString() {
    return String.format("%s%s%s", millis, SEPARATOR, nanoId);
  }
}