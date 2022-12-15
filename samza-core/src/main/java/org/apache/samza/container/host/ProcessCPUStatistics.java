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
 * A {@link ProcessCPUStatistics} object represents recent CPU usage percentage about the container process(including its child processes)
 */
public class ProcessCPUStatistics {

  /**
   * The CPU used by the Samza container process(including its child processes) in percentage.
   */
  private final double processCPUUsagePercentage;

  ProcessCPUStatistics(double processCpuUsagePercentage) {
    this.processCPUUsagePercentage = processCpuUsagePercentage;
  }

  public double getProcessCPUUsagePercentage() {
    return processCPUUsagePercentage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessCPUStatistics that = (ProcessCPUStatistics) o;
    return Double.compare(that.processCPUUsagePercentage, processCPUUsagePercentage) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(processCPUUsagePercentage);
  }

  @Override
  public String toString() {
    return "ProcessCPUStatistics{" + "processCPUUsagePercentage=" + processCPUUsagePercentage + '}';
  }
}
