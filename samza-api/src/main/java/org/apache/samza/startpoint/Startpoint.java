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
package org.apache.samza.startpoint;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.time.Instant;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Startpoint represents a position in a stream partition.
 */
@InterfaceStability.Evolving
public abstract class Startpoint {

  private final long creationTimestamp;

  Startpoint() {
    this(Instant.now().toEpochMilli());
  }

  Startpoint(long creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  /**
   * The timestamp when this {@link Startpoint} was written to the storage layer.
   * @return a timestamp in epoch milliseconds.
   */
  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  /**
   * Applies the {@link StartpointVisitor}'s visit methods to the {@link Startpoint}
   * and resolves it to a system specific offset.
   * @param systemStreamPartition the {@link SystemStreamPartition} of the startpoint.
   * @param startpointVisitor the visitor of the startpoint.
   * @return the resolved offset.
   */
  public abstract String apply(SystemStreamPartition systemStreamPartition, StartpointVisitor startpointVisitor);

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Startpoint that = (Startpoint) o;
    return Objects.equal(creationTimestamp, that.creationTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(creationTimestamp);
  }
}
