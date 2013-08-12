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

public class SystemStreamPartition extends SystemStream {
  protected final Partition partition;

  public SystemStreamPartition(String system, String stream, Partition partition) {
    super(system, stream);
    this.partition = partition;
  }

  public SystemStreamPartition(SystemStreamPartition other) {
    this(other.getSystem(), other.getStream(), other.getPartition());
  }

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
    return "SystemStreamPartition [partition=" + partition + ", system=" + system + ", stream=" + stream + "]";
  }
}
