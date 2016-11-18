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
package org.apache.samza.container.host;

/**
 * A {@link SystemMemoryStatistics} object represents information about the physical process that runs the
 * {@link org.apache.samza.container.SamzaContainer}.
 */
public class SystemMemoryStatistics {

  /**
   * The physical memory used by the Samza container process (native + on heap) in bytes.
   */
  private final long physicalMemoryBytes;

  SystemMemoryStatistics(long physicalMemoryBytes) {
    this.physicalMemoryBytes = physicalMemoryBytes;
  }

  @Override
  public String toString() {
    return "SystemStatistics{" +
        "containerPhysicalMemory=" + physicalMemoryBytes +
        '}';
  }

  public long getPhysicalMemoryBytes() {
    return physicalMemoryBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SystemMemoryStatistics that = (SystemMemoryStatistics) o;

    return physicalMemoryBytes == that.physicalMemoryBytes;

  }

  @Override
  public int hashCode() {
    return (int) (physicalMemoryBytes ^ (physicalMemoryBytes >>> 32));
  }
}
