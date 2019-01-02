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
import org.apache.samza.system.SystemStreamPartition;


/**
 * A {@link Startpoint} that represents a specific offset in a stream partition.
 */
public final class StartpointSpecific extends Startpoint {

  private final String specificOffset;

  // Default constructor needed by serde.
  private StartpointSpecific() {
    this(null);
  }

  /**
   * Constructs a {@link Startpoint} that represents a specific offset in a stream partition.
   * @param specificOffset Specific offset in a stream partition.
   */
  public StartpointSpecific(String specificOffset) {
    super();
    this.specificOffset = specificOffset;
  }

  /**
   * Getter for the specific offset.
   * @return the specific offset.
   */
  public String getSpecificOffset() {
    return specificOffset;
  }

  @Override
  public void apply(SystemStreamPartition systemStreamPartition, StartpointVisitor startpointVisitor) {
    startpointVisitor.visit(systemStreamPartition, this);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("specificOffset", specificOffset).toString();
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
    StartpointSpecific that = (StartpointSpecific) o;
    return Objects.equal(specificOffset, that.specificOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), specificOffset);
  }
}
