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
package org.apache.samza.rest.model;

import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Provides a client view of the samza system stream partition.
 * Includes the system name, stream name and partition id
 */
public class Partition {

  private String system;
  private String stream;
  private int partitionId;

  public Partition() {
  }

  public Partition(@JsonProperty("system") String system,
                   @JsonProperty("stream") String stream,
                   @JsonProperty("partitionId") int partitionId) {
    this.system = system;
    this.stream = stream;
    this.partitionId = partitionId;
  }

  public Partition(SystemStreamPartition systemStreamPartition) {
    this(systemStreamPartition.getSystem(),
         systemStreamPartition.getStream(),
         systemStreamPartition.getPartition().getPartitionId());
  }

  public String getSystem() {
    return system;
  }

  public void setSystem(String system) {
    this.system = system;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Partition)) {
      return false;
    }

    Partition partition = (Partition) o;

    if (partitionId != partition.partitionId) {
      return false;
    }
    if (!system.equals(partition.system)) {
      return false;
    }
    return stream.equals(partition.stream);
  }

  @Override
  public int hashCode() {
    int result = system.hashCode();
    result = 31 * result + stream.hashCode();
    result = 31 * result + partitionId;
    return result;
  }
}
