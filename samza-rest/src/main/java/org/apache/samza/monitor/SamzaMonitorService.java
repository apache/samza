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
package org.apache.samza.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.rest.SamzaRestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.samza.monitor.MonitorConfig.getMonitorConfigs;
import static org.apache.samza.monitor.MonitorLoader.instantiateMonitor;


/**
 * The class responsible for handling long-running/scheduled monitors in Samza REST.
 * Takes a SamzaRestConfig object in the constructor and handles instantiation of
 * monitors and scheduling them to run based on the properties in the config.
 */
public class SamzaMonitorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SamzaMonitorService.class);
  private static final SecureRandom RANDOM = new SecureRandom();

  private final SamzaRestConfig config;
  private final MetricsRegistry metricsRegistry;
  private final List<ScheduledExecutorService> scheduledExecutors;
  public SamzaMonitorService(SamzaRestConfig config,
      MetricsRegistry metricsRegistry) {
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.scheduledExecutors = new ArrayList<>();
  }

  public void start() {
    try {
      Map<String, MonitorConfig> monitorConfigs = getMonitorConfigs(config);
      for (Map.Entry<String, MonitorConfig> entry : monitorConfigs.entrySet()) {
        String monitorName = entry.getKey();
        MonitorConfig monitorConfig = entry.getValue();

        if (!Strings.isNullOrEmpty(monitorConfig.getMonitorFactoryClass())) {
          int schedulingIntervalInMs = monitorConfig.getSchedulingIntervalInMs();
          int monitorSchedulingJitterInMs = (int) (RANDOM.nextInt(schedulingIntervalInMs + 1) * (monitorConfig.getSchedulingJitterPercent() / 100.0));
          schedulingIntervalInMs += monitorSchedulingJitterInMs;
          LOGGER.info("Scheduling the monitor: {} to run every {} ms.", monitorName, schedulingIntervalInMs);
          // Create a new SchedulerExecutorService for each monitor. This ensures that a long running monitor service
          // does not block another monitor from scheduling/running. A long running monitor will not create a backlog
          // of work for future execution of the same monitor. A new monitor is scheduled only when current work is complete.
          createSchedulerAndScheduleMonitor(monitorName, monitorConfig, schedulingIntervalInMs);
        } else {
          // When MonitorFactoryClass is not defined in the config, ignore the monitor config
          LOGGER.warn("Not scheduling the monitor: {} to run, since monitor factory class is not set in config.", monitorName);
        }
      }
    } catch (InstantiationException e) {
      LOGGER.error("Exception when instantiating the monitor : ", e);
      throw new SamzaException(e);
    }
  }

  public void stop() {
    scheduledExecutors.forEach(ExecutorService::shutdown);
  }

  private Runnable getRunnable(final Monitor monitor) {
    return new Runnable() {
      public void run() {
        try {
          monitor.monitor();
        } catch (IOException e) {
          LOGGER.error("Caught IOException during " + monitor.toString() + ".monitor()", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error("Caught InterruptedException during " + monitor.toString() + ".monitor()", e);
        } catch (Exception e) {
          LOGGER.error("Unexpected exception during {}.monitor()", monitor, e);
        }
      }
    };
  }

  /**
   * Creates a ScheduledThreadPoolExecutor with core pool size 1 and schedules the monitor to run every schedulingIntervalInMs
   */
  @VisibleForTesting
  public void createSchedulerAndScheduleMonitor(String monitorName, MonitorConfig monitorConfig, long schedulingIntervalInMs)
      throws InstantiationException {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("MonitorThread-%d")
        .build();

    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, threadFactory);
    scheduledExecutors.add(scheduledExecutorService);

    scheduledExecutorService
        .scheduleAtFixedRate(getRunnable(instantiateMonitor(monitorName, monitorConfig, metricsRegistry)),
            0, schedulingIntervalInMs, TimeUnit.MILLISECONDS);
  }
}
