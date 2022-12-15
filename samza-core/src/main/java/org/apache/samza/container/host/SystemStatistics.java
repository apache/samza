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

import java.util.Objects;

/**
 * A {@link SystemStatistics} object represents system related information about the physical process that runs the
 * {@link org.apache.samza.container.SamzaContainer}.
 */
public class SystemStatistics {

  private final ProcessCPUStatistics cpuStatistics;
  private final SystemMemoryStatistics memoryStatistics;

  public SystemStatistics(ProcessCPUStatistics cpuStatistics, SystemMemoryStatistics memoryStatistics) {
    this.cpuStatistics = cpuStatistics;
    this.memoryStatistics = memoryStatistics;
  }

  public ProcessCPUStatistics getCpuStatistics() {
    return cpuStatistics;
  }

  public SystemMemoryStatistics getMemoryStatistics() {
    return memoryStatistics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SystemStatistics that = (SystemStatistics) o;
    return Objects.equals(cpuStatistics, that.cpuStatistics) && Objects.equals(memoryStatistics, that.memoryStatistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cpuStatistics, memoryStatistics);
  }

  @Override
  public String toString() {
    return "SystemStatistics{" + "cpuStatistics=" + cpuStatistics + ", memoryStatistics=" + memoryStatistics + '}';
  }
}
