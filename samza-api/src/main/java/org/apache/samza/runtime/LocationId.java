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
package org.apache.samza.runtime;

import java.util.Objects;

/**
 * Represents the physical execution environment of the StreamProcessor.
 * All the stream processors which run from a LocationId should be able to share (read/write)
 * their local state stores.
 */
public class LocationId {
  private final String locationId;

  public LocationId(String locationId) {
    if (locationId == null) {
      throw new IllegalArgumentException("LocationId cannot be null");
    }
    this.locationId = locationId;
  }

  public String getId() {
    return this.locationId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LocationId that = (LocationId) o;

    return Objects.equals(locationId, that.locationId);
  }

  @Override
  public int hashCode() {
    return locationId.hashCode();
  }

  @Override
  public String toString() {
    return locationId;
  }
}
