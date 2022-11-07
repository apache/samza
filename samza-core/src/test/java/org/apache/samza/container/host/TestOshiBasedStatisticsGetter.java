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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestOshiBasedStatisticsGetter {
  @Mock
  private OperatingSystem os;
  @Mock
  private OSProcess process;
  @Mock
  private OSProcess childProcess;
  @Mock
  private OSProcess processSnapshot;
  @Mock
  private OSProcess childProcessSnapshot;

  private final int pid = 10001;
  private final int childPid = 10002;
  private final int cpuCount = 2;

  private OshiBasedStatisticsGetter oshiBasedStatisticsGetter;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(process.getProcessID()).thenReturn(pid);
    when(processSnapshot.getProcessID()).thenReturn(pid);
    when(childProcess.getProcessID()).thenReturn(childPid);
    when(childProcessSnapshot.getProcessID()).thenReturn(childPid);

    when(os.getProcessId()).thenReturn(pid);

    oshiBasedStatisticsGetter = new OshiBasedStatisticsGetter(os, cpuCount);
  }

  @Test
  public void testGetProcessCPUStatistics() {
    // first time to get cpu usage info
    double processCpuUsage1 = 0.5;
    double childProcessCPUUsage1 = 0.3;
    when(os.getProcess(pid)).thenReturn(processSnapshot);
    when(os.getProcess(childPid)).thenReturn(childProcessSnapshot);
    when(os.getChildProcesses(pid, OperatingSystem.ProcessFiltering.ALL_PROCESSES,
        OperatingSystem.ProcessSorting.NO_SORTING, 0)).thenReturn(Lists.newArrayList(childProcessSnapshot));
    when(processSnapshot.getProcessCpuLoadBetweenTicks(null)).thenReturn(processCpuUsage1);
    when(childProcessSnapshot.getProcessCpuLoadBetweenTicks(null)).thenReturn(childProcessCPUUsage1);
    assertEquals(new ProcessCPUStatistics(100d * (processCpuUsage1 + childProcessCPUUsage1) / cpuCount),
        oshiBasedStatisticsGetter.getProcessCPUStatistics());

    // second time to get cpu usage info
    double processCpuUsage2 = 0.2;
    double childProcessCPUUsage2 = 2;
    when(os.getProcess(pid)).thenReturn(process);
    when(os.getProcess(childPid)).thenReturn(childProcess);
    when(os.getChildProcesses(pid, OperatingSystem.ProcessFiltering.ALL_PROCESSES,
        OperatingSystem.ProcessSorting.NO_SORTING, 0)).thenReturn(Lists.newArrayList(childProcess));
    when(process.getProcessCpuLoadBetweenTicks(processSnapshot)).thenReturn(processCpuUsage2);
    when(childProcess.getProcessCpuLoadBetweenTicks(childProcessSnapshot)).thenReturn(childProcessCPUUsage2);
    assertEquals(new ProcessCPUStatistics(100d * (processCpuUsage2 + childProcessCPUUsage2) / cpuCount),
        oshiBasedStatisticsGetter.getProcessCPUStatistics());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetSystemMemoryStatistics() {
    oshiBasedStatisticsGetter.getSystemMemoryStatistics();
  }
}
