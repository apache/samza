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
 * Aggregate object representing a portion of {@link SystemStreamPartition} consisting of envelopes within the
 * SystemStreamPartition that have envelope.key % job's elasticity factor = keyHash of this object.
 */
public class SystemStreamPartitionKeyHash extends SystemStreamPartition {
  protected final int keyHash;

  /**
   * Constructs a Samza stream partition KeyHash object from specified components.
   * @param system The name of the system of which this stream is associated with.
   * @param stream The name of the stream as specified in the stream configuration file.
   * @param partition The partition in the stream of which this object is associated with.
   * @param keyHash The KeyHash of the partition (aka portion of SSP) this object is associated with.
   */
  public SystemStreamPartitionKeyHash(String system, String stream, Partition partition, int keyHash) {
    super(system, stream, partition);
    this.keyHash = keyHash;
  }
  /**
   * Constructs a Samza stream partition object based upon an existing Samza stream partition and a keyHash.
   * @param systemStreamPartition Reference to an already existing Samza stream partition.
   * @param keyHash the KeyHash of the systemStreamPartition for this object.
   */
  public SystemStreamPartitionKeyHash(SystemStreamPartition systemStreamPartition, int keyHash) {
    super(systemStreamPartition);
    this.keyHash = keyHash;
  }

  /**
   * Constructs a Samza stream partition KeyHash object based upon an existing Samza stream partition keyHash.
   * @param other Reference to an already existing Samza stream partition KeyHash.
   */
  public SystemStreamPartitionKeyHash(SystemStreamPartitionKeyHash other) {
    this(other.getSystem(), other.getStream(), other.getPartition(), other.keyHash);
  }

  /**
   * Constructs a Samza stream partition object based upon another Samza stream and a specified partition.
   * @param other Reference to an already existing Samza stream.
   * @param partition Reference to an already existing Samza partition.
   * @param keyHash  the KeyHash of the systemStreamPartition for this object.
   */
  public SystemStreamPartitionKeyHash(SystemStream other, Partition partition, int keyHash) {
    super(other, partition);
    this.keyHash = keyHash;
  }

  public int getKeyHash() {
    return this.keyHash;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  private int computeHashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + keyHash;
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
    SystemStreamPartitionKeyHash other = (SystemStreamPartitionKeyHash) obj;
    if (keyHash != other.keyHash) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "SystemStreamPartitionKeyHash [" + system + ", " + stream + ", " + partition.getPartitionId() + ", " + keyHash + "]";
  }

  @Override
  public int compareTo(SystemStreamPartition that) {
    SystemStreamPartitionKeyHash other = (SystemStreamPartitionKeyHash) that;
    if (this.system.compareTo(other.system) < 0) {
      return -1;
    } else if (this.system.compareTo(other.system) > 0) {
      return 1;
    }

    if (this.stream.compareTo(other.stream) < 0) {
      return -1;
    } else if (this.stream.compareTo(other.stream) > 0) {
      return 1;
    }

    if (this.partition.compareTo(other.partition) < 0) {
      return -1;
    } else if (this.partition.compareTo(other.partition) > 0) {
      return 1;
    }

    if (this.keyHash < other.keyHash) {
      return -1;
    } else if (this.keyHash > other.keyHash) {
      return 1;
    }

    return 0;
  }
}
