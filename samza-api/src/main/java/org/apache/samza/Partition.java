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

package org.apache.samza;

/**
 * A numbered, ordered partition of a stream.
 */
public class Partition {
  private final int partition;

  /**
   * Constructs a new Samza stream partition from a specified partition number.
   * @param partition An integer identifying the partition in a Samza stream.
   */
  public Partition(int partition) {
    this.partition = partition;
  }

  public int getPartitionId() {
    return partition;
  }

  @Override
  public int hashCode() {
    return partition;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Partition other = (Partition) obj;
    if (partition != other.partition)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Partition [partition=" + partition + "]";
  }
}
