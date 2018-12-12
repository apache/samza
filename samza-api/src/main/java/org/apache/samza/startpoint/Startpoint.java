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

import com.google.common.base.Objects;
import org.apache.samza.annotation.InterfaceStability;

import static org.apache.samza.startpoint.PositionType.*;


/**
 * Startpoint represents a position in a stream by {@link PositionType}
 */
@InterfaceStability.Unstable
public class Startpoint {
  private final Long storedAt;
  private final PositionType positionType;
  private final String position;

  Startpoint(PositionType positionType, String position) {
    this(positionType, position, null);
  }

  Startpoint(PositionType positionType, String position, Long storedAt) {
    this.positionType = positionType;
    this.position = position;
    this.storedAt = storedAt;
  }

  /**
   * Creates a {@link Startpoint} with a specific offset.
   * @param offset The specific offset.
   * @return a Startpoint with a specific offset.
   */
  public static Startpoint withSpecificOffset(String offset) {
    return new Startpoint(SPECIFIC_OFFSET, offset);
  }

  /**
   * Creates a {@link Startpoint} with timestamp offset.
   * @param timestamp The timestamp offset.
   * @return a Startpoint with a timestamp offset.
   */
  public static Startpoint withTimestamp(long timestamp) {
    return new Startpoint(TIMESTAMP, Long.toString(timestamp));
  }

  /**
   * Creates a {@link Startpoint} the describes the earliest offset.
   * @return a Startpoint describing the earliest offset.
   */
  public static Startpoint withEarliest() {
    return new Startpoint(EARLIEST, null);
  }

  /**
   * Creates a {@link Startpoint} the describes the latest offset.
   * @return a Startpoint describing the latest offset.
   */
  public static Startpoint withLatest() {
    return new Startpoint(LATEST, null);
  }

  /**
   * Creates a {@link Startpoint} to trigger a stream to bootstrap.
   * @param bootstrapInfo Any additional information needed to bootstrap.
   * @return a Startpoint to trigger bootstrap on a stream.
   */
  public static Startpoint withBootstrap(String bootstrapInfo) {
    return new Startpoint(BOOTSTRAP, bootstrapInfo);
  }

  /**
   * Provides semantics for the {@link #getPosition()} value.
   * @return {@link PositionType}
   */
  public PositionType getPositionType() {
    return positionType;
  }

  /**
   * Position value described by {@link #getPosition()}.
   * @return the position value.
   */
  public String getPosition() {
    return position;
  }

  /**
   * The timestamp when this {@link Startpoint} was written to the storage layer.
   * @return a timestamp in epoch milliseconds.
   */
  public Long getStoredAt() {
    return storedAt;
  }

  Startpoint copyWithStoredAt(Long storedAt) {
    return new Startpoint(positionType, position, storedAt);
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
    return Objects.equal(storedAt, that.storedAt) && positionType == that.positionType && Objects.equal(position,
        that.position);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(storedAt, positionType, position);
  }
}
