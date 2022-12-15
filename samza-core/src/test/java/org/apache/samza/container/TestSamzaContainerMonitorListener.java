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
package org.apache.samza.container;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.host.ProcessCPUStatistics;
import org.apache.samza.container.host.SystemMemoryStatistics;
import org.apache.samza.container.host.SystemStatistics;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestSamzaContainerMonitorListener {

  @Mock
  private ProcessCPUStatistics cpuSample;
  @Mock
  private SystemMemoryStatistics memorySample;
  @Mock
  private ThreadPoolExecutor taskThreadPool;

  private SystemStatistics sample;

  private final double cpuUsage = 30.0;
  private final int containerMemoryMb = 2048;
  private final long physicalMemoryBytes = 1024000L;
  private final int activeThreadCount = 2;

  private final Config config =
      new MapConfig(Collections.singletonMap("cluster-manager.container.memory.mb", String.valueOf(containerMemoryMb)));
  private final SamzaContainerMetrics containerMetrics =
      new SamzaContainerMetrics("container", new MetricsRegistryMap(), "");

  private SamzaContainerMonitorListener samzaContainerMonitorListener;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(cpuSample.getProcessCPUUsagePercentage()).thenReturn(cpuUsage);
    when(memorySample.getPhysicalMemoryBytes()).thenReturn(physicalMemoryBytes);
    when(taskThreadPool.getActiveCount()).thenReturn(activeThreadCount);

    sample = new SystemStatistics(cpuSample, memorySample);
    samzaContainerMonitorListener = new SamzaContainerMonitorListener(config, containerMetrics, taskThreadPool);
  }

  @Test
  public void testOnUpdate() {
    samzaContainerMonitorListener.onUpdate(sample);
    assertEquals(cpuUsage, containerMetrics.totalProcessCpuUsage().getValue());
    float physicalMemoryMb = physicalMemoryBytes / 1024.0F / 1024.0F;
    assertEquals(physicalMemoryMb, containerMetrics.physicalMemoryMb().getValue());
    assertEquals(physicalMemoryMb / containerMemoryMb, containerMetrics.physicalMemoryUtilization().getValue());
    assertEquals(activeThreadCount, containerMetrics.containerActiveThreads().getValue());
  }
}
