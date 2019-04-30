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
import org.apache.samza.system.SystemStreamPartition;


/**
 * A {@link Startpoint} that represents a timestamp offset in a stream partition.
 */
public final class StartpointTimestamp extends Startpoint {

  private final Long timestampOffset;

  // Default constructor needed by serde.
  private StartpointTimestamp() {
    this(null);
  }

  /**
   * Constructs a {@link Startpoint} that represents a timestamp offset in a stream partition.
   * @param timestampOffset in a stream partition in milliseconds.
   */
  public StartpointTimestamp(Long timestampOffset) {
    super();
    this.timestampOffset = timestampOffset;
  }

  /**
   * Getter for the timestamp offset.
   * @return the timestamp offset in milliseconds.
   */
  public Long getTimestampOffset() {
    return timestampOffset;
  }

  @Override
  public String apply(SystemStreamPartition systemStreamPartition, StartpointVisitor startpointVisitor) {
    return startpointVisitor.visit(systemStreamPartition, this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("timestampOffset", timestampOffset).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    StartpointTimestamp that = (StartpointTimestamp) o;
    return Objects.equal(timestampOffset, that.timestampOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), timestampOffset);
  }
}
