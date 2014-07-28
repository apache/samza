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

import org.apache.samza.system.SystemStreamPartition;

import java.util.Collections;
import java.util.Map;

/**
 * A checkpoint is a mapping of all the streams a job is consuming and the most recent current offset for each.
 * It is used to restore a {@link org.apache.samza.task.StreamTask}, either as part of a job restart or as part
 * of restarting a failed container within a running job.
 */
public class Checkpoint {
  private final Map<SystemStreamPartition, String> offsets;

  /**
   * Constructs a new checkpoint based off a map of Samza stream offsets.
   * @param offsets Map of Samza streams to their current offset.
   */
  public Checkpoint(Map<SystemStreamPartition, String> offsets) {
    this.offsets = offsets;
  }

  /**
   * Gets a unmodifiable view of the current Samza stream offsets.
   * @return A unmodifiable view of a Map of Samza streams to their recorded offsets.
   */
  public Map<SystemStreamPartition, String> getOffsets() {
    return Collections.unmodifiableMap(offsets);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Checkpoint)) return false;

    Checkpoint that = (Checkpoint) o;

    if (offsets != null ? !offsets.equals(that.offsets) : that.offsets != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return offsets != null ? offsets.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Checkpoint [offsets=" + offsets + "]";
  }
}
