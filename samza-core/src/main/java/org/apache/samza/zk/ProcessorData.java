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

package org.apache.samza.zk;

import java.util.Objects;
import org.apache.samza.SamzaException;
import org.apache.samza.runtime.LocationId;

/**
 * Represents processor data stored in zookeeper processors node.
 */
public class ProcessorData {
  private final String processorId;
  private final String locationId;

  public ProcessorData(String locationId, String processorId) {
    this.processorId = processorId;
    this.locationId = locationId;
  }

  public ProcessorData(String data) {
    String[] splt = data.split(" ");
    if (splt.length != 2) {
      throw new SamzaException("incorrect processor data format = " + data);
    }
    locationId = splt[0];
    processorId = splt[1];
  }

  public String toString() {
    return locationId + " " + processorId;
  }

  public LocationId getLocationId() {
    return new LocationId(locationId);
  }

  public String getProcessorId() {
    return processorId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(processorId, locationId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    final ProcessorData other = (ProcessorData) obj;
    return Objects.equals(processorId, other.processorId) && Objects.equals(locationId, other.locationId);
  }
}
