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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.container.host.ProcessCPUStatistics;
import org.apache.samza.container.host.SystemMemoryStatistics;
import org.apache.samza.container.host.SystemStatistics;
import org.apache.samza.container.host.SystemStatisticsMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaContainerMonitorListener implements SystemStatisticsMonitor.Listener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SamzaContainerMonitorListener.class);

  private final SamzaContainerMetrics containerMetrics;
  private final ExecutorService taskThreadPool;
  private final int containerMemoryMb;

  public SamzaContainerMonitorListener(Config config, SamzaContainerMetrics containerMetrics,
      ExecutorService taskThreadPool) {
    this.containerMetrics = containerMetrics;
    this.taskThreadPool = taskThreadPool;
    this.containerMemoryMb = new ClusterManagerConfig(config).getContainerMemoryMb();
  }

  @Override
  public void onUpdate(SystemStatistics sample) {
    // update cpu metric
    ProcessCPUStatistics cpuSample = sample.getCpuStatistics();
    if (Objects.nonNull(cpuSample)) {
      double cpuUsage = cpuSample.getProcessCPUUsagePercentage();
      LOGGER.debug("Container total cpu usage: " + cpuUsage);
      containerMetrics.totalProcessCpuUsage().set(cpuUsage);
    }

    // update memory metric
    SystemMemoryStatistics memorySample = sample.getMemoryStatistics();
    if (Objects.nonNull(memorySample)) {
      long physicalMemoryBytes = memorySample.getPhysicalMemoryBytes();
      float physicalMemoryMb = physicalMemoryBytes / (1024.0F * 1024.0F);
      float memoryUtilization = physicalMemoryMb / containerMemoryMb;
      LOGGER.debug("Container physical memory utilization (mb): " + physicalMemoryMb);
      LOGGER.debug("Container physical memory utilization: " + memoryUtilization);
      containerMetrics.physicalMemoryMb().set(physicalMemoryMb);
      containerMetrics.physicalMemoryUtilization().set(memoryUtilization);
    }

    // update thread related metrics
    if (Objects.nonNull(taskThreadPool) && taskThreadPool instanceof ThreadPoolExecutor) {
      int containerActiveThreads = ((ThreadPoolExecutor) taskThreadPool).getActiveCount();
      LOGGER.debug("Container active threads count: " + containerActiveThreads);
      containerMetrics.containerActiveThreads().set(containerActiveThreads);
    }
  }
}
