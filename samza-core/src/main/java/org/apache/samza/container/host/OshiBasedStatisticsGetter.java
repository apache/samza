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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.annotation.concurrent.NotThreadSafe;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;


/**
 * An implementation of {@link SystemStatisticsGetter} that relies on using oshi framework(https://www.oshi.ooo/)
 *
 * This class captures the recent cpu usage percentage(in the [0, 100] interval) used by the Samza container process and
 * its child processes. It gets CPU usage of this process since a previous snapshot of the same process, the snapshot is
 * triggered by last poll, have polling interval of at least a few seconds is recommended.
 */
@NotThreadSafe
public class OshiBasedStatisticsGetter implements SystemStatisticsGetter {
  private static final Logger LOGGER = LoggerFactory.getLogger(OshiBasedStatisticsGetter.class);
  // the snapshots of current JVM process and its child processes
  private final Map<Integer, OSProcess> previousProcessSnapshots = new HashMap<>();

  private final OperatingSystem os;
  private final int cpuCount;

  public OshiBasedStatisticsGetter() {
    this(new SystemInfo());
  }

  @VisibleForTesting
  OshiBasedStatisticsGetter(SystemInfo si) {
    this(si.getOperatingSystem(), si.getHardware().getProcessor().getLogicalProcessorCount());
  }

  @VisibleForTesting
  OshiBasedStatisticsGetter(OperatingSystem os, int cpuCount) {
    this.os = os;
    this.cpuCount = cpuCount;
  }

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ProcessCPUStatistics getProcessCPUStatistics() {
    try {
      final List<OSProcess> currentProcessAndChildProcesses = getCurrentProcessAndChildProcesses();
      final double totalCPUUsage = getTotalCPUUsage(currentProcessAndChildProcesses);
      refreshProcessSnapshots(currentProcessAndChildProcesses);
      return new ProcessCPUStatistics(100d * totalCPUUsage / cpuCount);
    } catch (Exception e) {
      LOGGER.warn("Error when running oshi: ", e);
      return null;
    }
  }

  private List<OSProcess> getCurrentProcessAndChildProcesses() {
    final List<OSProcess> processes = new ArrayList<>();
    // get current process
    processes.add(os.getProcess(os.getProcessId()));
    // get all child processes of current process
    processes.addAll(os.getChildProcesses(os.getProcessId(), OperatingSystem.ProcessFiltering.ALL_PROCESSES,
        OperatingSystem.ProcessSorting.NO_SORTING, 0));
    return processes;
  }

  private double getTotalCPUUsage(List<OSProcess> processes) {
    return processes.stream()
        .mapToDouble(p -> p.getProcessCpuLoadBetweenTicks(previousProcessSnapshots.get(p.getProcessID())))
        .sum();
  }

  private void refreshProcessSnapshots(List<OSProcess> processes) {
    previousProcessSnapshots.clear();
    processes.forEach(p -> previousProcessSnapshots.put(p.getProcessID(), p));
  }
}
