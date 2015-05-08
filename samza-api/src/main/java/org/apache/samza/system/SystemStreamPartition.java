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

package org.apache.samza.system;

import org.apache.samza.Partition;

/**
 * Aggregate object representing a both the {@link org.apache.samza.system.SystemStream} and {@link org.apache.samza.Partition}.
 */
public class SystemStreamPartition extends SystemStream implements Comparable<SystemStreamPartition> {
  protected final Partition partition;
  protected final int hash;  // precomputed as instances are immutable and often stored in hash-addressed data structures

  /**
   * Constructs a Samza stream partition object from specified components.
   * @param system The name of the system of which this stream is associated with.
   * @param stream The name of the stream as specified in the stream configuration file.
   * @param partition The partition in the stream of which this object is associated with.
   */
  public SystemStreamPartition(String system, String stream, Partition partition) {
    super(system, stream);
    this.partition = partition;
    this.hash = computeHashCode();
  }

  /**
   * Constructs a Samza stream partition object based upon an existing Samza stream partition.
   * @param other Reference to an already existing Samza stream partition.
   */
  public SystemStreamPartition(SystemStreamPartition other) {
    this(other.getSystem(), other.getStream(), other.getPartition());
  }

  /**
   * Constructs a Samza stream partition object based upon another Samza stream and a specified partition.
   * @param other Reference to an already existing Samza stream.
   * @param partition Reference to an already existing Samza partition.
   */
  public SystemStreamPartition(SystemStream other, Partition partition) {
    this(other.getSystem(), other.getStream(), partition);
  }

  public Partition getPartition() {
    return partition;
  }
  
  public SystemStream getSystemStream() {
    return new SystemStream(system, stream);
  }

  @Override
  public int hashCode() {
    return hash;
  }
  
  private int computeHashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((partition == null) ? 0 : partition.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SystemStreamPartition other = (SystemStreamPartition) obj;
    if (partition == null) {
      if (other.partition != null)
        return false;
    } else if (!partition.equals(other.partition))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SystemStreamPartition [" + system + ", " + stream + ", " + partition.getPartitionId() + "]";
  }

  @Override
  public int compareTo(SystemStreamPartition that) {
    if (this.system.compareTo(that.system) < 0) {
      return -1;
    } else if (this.system.compareTo(that.system) > 0) {
      return 1;
    }

    if (this.stream.compareTo(that.stream) < 0) {
      return -1;
    } else if (this.stream.compareTo(that.stream) > 0) {
      return 1;
    }

    if (this.partition.compareTo(that.partition) < 0) {
      return -1;
    } else if (this.partition.compareTo(that.partition) > 0) {
      return 1;
    }
    return 0;
  }
}
