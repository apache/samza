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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.coordinator.CoordinationConstants;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerHeartbeatMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerHeartbeatMonitor.class);
  private static final ThreadFactory THREAD_FACTORY = new HeartbeatThreadFactory();
  private static final CoordinatorStreamValueSerde SERDE = new CoordinatorStreamValueSerde(SetConfig.TYPE);
  private static final String SOURCE_NAME = "SamzaContainer";

  @VisibleForTesting
  static final int SCHEDULE_MS = 60000;
  @VisibleForTesting
  static final int SHUTDOWN_TIMOUT_MS = 120000;

  private final Runnable onContainerExpired;
  private final ScheduledExecutorService scheduler;
  private final String containerExecutionId;
  private final MetadataStore coordinatorStreamStore;
  private final long sleepDurationForReconnectWithAM;
  private final boolean isApplicationMasterHighAvailabilityEnabled;
  private final long retryCount;

  private ContainerHeartbeatClient containerHeartbeatClient;
  private ContainerHeartbeatMetrics metrics;
  private Map<String, MetricsReporter> reporters;
  private String coordinatorUrl;
  private boolean started = false;

  public ContainerHeartbeatMonitor(Runnable onContainerExpired, String coordinatorUrl, String containerExecutionId,
      MetadataStore coordinatorStreamStore, Config config) {
    this(onContainerExpired, new ContainerHeartbeatClient(coordinatorUrl, containerExecutionId),
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY), coordinatorUrl, containerExecutionId,
        coordinatorStreamStore, config);
  }

  @VisibleForTesting
  ContainerHeartbeatMonitor(Runnable onContainerExpired, ContainerHeartbeatClient containerHeartbeatClient,
      ScheduledExecutorService scheduler, String coordinatorUrl, String containerExecutionId,
      MetadataStore coordinatorStreamStore, Config config) {
    this.onContainerExpired = onContainerExpired;
    this.containerHeartbeatClient = containerHeartbeatClient;
    this.scheduler = scheduler;
    this.coordinatorUrl = coordinatorUrl;
    this.containerExecutionId = containerExecutionId;
    this.coordinatorStreamStore = coordinatorStreamStore;

    JobConfig jobConfig = new JobConfig(config);
    this.isApplicationMasterHighAvailabilityEnabled = jobConfig.getApplicationMasterHighAvailabilityEnabled();
    this.retryCount = jobConfig.getContainerHeartbeatRetryCount();
    this.sleepDurationForReconnectWithAM = jobConfig.getContainerHeartbeatRetrySleepDurationMs();

    initializeMetrics(config);
  }

  public void start() {
    if (started) {
      LOG.warn("Skipping attempt to start an already started ContainerHeartbeatMonitor.");
      return;
    }
    LOG.info("Starting ContainerHeartbeatMonitor");
    scheduler.scheduleAtFixedRate(() -> {
      ContainerHeartbeatResponse response = containerHeartbeatClient.requestHeartbeat();
      if (!response.isAlive()) {
        metrics.incrementHeartbeatExpiredCount();
        if (isApplicationMasterHighAvailabilityEnabled) {
          LOG.warn("Failed to establish connection with {}. Checking for new AM", coordinatorUrl);
          try {
            if (checkAndEstablishConnectionWithNewAM()) {
              return;
            }
          } catch (Exception e) {
            // On exception in re-establish connection with new AM, force exit.
            LOG.error("Exception trying to connect with new AM", e);
            metrics.incrementHeartbeatEstablishedFailureCount();
            forceExit("failure in establishing connection with new AM", SHUTDOWN_TIMOUT_MS);
            return;
          }
        }
        // On timeout of container shutting down, force exit.
        forceExit("Graceful shutdown timeout expired. Force exiting.", SHUTDOWN_TIMOUT_MS);
      }
    }, 0, SCHEDULE_MS, TimeUnit.MILLISECONDS);
    started = true;
  }

  public void stop() {
    if (started) {
      LOG.info("Stopping ContainerHeartbeatMonitor");
      scheduler.shutdown();
      reporters.values().forEach(MetricsReporter::stop);
    }
  }

  private boolean checkAndEstablishConnectionWithNewAM() {
    boolean response = false;
    int attempt = 1;

    long startTime = System.currentTimeMillis();
    while (attempt <= retryCount) {
      String newCoordinatorUrl = SERDE.fromBytes(coordinatorStreamStore.get(CoordinationConstants.YARN_COORDINATOR_URL));
      try {
        if (coordinatorUrl.equals(newCoordinatorUrl)) {
          LOG.info("Attempt {} to discover new AM. Sleep for {}ms before next attempt.", attempt, sleepDurationForReconnectWithAM);
          Thread.sleep(sleepDurationForReconnectWithAM);
        } else {
          LOG.info("Found new AM: {}. Establishing heartbeat with the new AM.", newCoordinatorUrl);
          coordinatorUrl = newCoordinatorUrl;
          containerHeartbeatClient = createContainerHeartbeatClient(coordinatorUrl, containerExecutionId);
          response = containerHeartbeatClient.requestHeartbeat().isAlive();
          LOG.info("Received heartbeat response: {} from new AM: {}", response, this.coordinatorUrl);
          break;
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during sleep.");
        throw new SamzaException(e);
      }
      attempt++;
    }

    metrics.setHeartbeatDiscoveryTime(System.currentTimeMillis() - startTime);
    if (response) {
      metrics.incrementHeartbeatEstablishedWithNewAmCount();
    } else {
      metrics.incrementHeartbeatEstablishedFailureCount();
    }

    return response;
  }

  private void initializeMetrics(Config config) {
    reporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), SOURCE_NAME);
    MetricsRegistryMap registryMap = new MetricsRegistryMap();
    metrics = new ContainerHeartbeatMetrics(registryMap);
    reporters.values().forEach(reporter -> reporter.register(SOURCE_NAME, registryMap));
  }

  @VisibleForTesting
  ContainerHeartbeatClient createContainerHeartbeatClient(String coordinatorUrl, String containerExecutionId) {
    return new ContainerHeartbeatClient(coordinatorUrl, containerExecutionId);
  }

  @VisibleForTesting
  ContainerHeartbeatMetrics getMetrics() {
    return metrics;
  }

  private void forceExit(String message, int timeout) {
    scheduler.schedule(() -> {
      if (started) {
        reporters.values().forEach(MetricsReporter::stop);
      }

      LOG.error(message);
      ThreadUtil.logThreadDump("Thread dump at heartbeat monitor: due to " + message);
      System.exit(1);
    }, timeout, TimeUnit.MILLISECONDS);
    onContainerExpired.run();
  }

  private static class HeartbeatThreadFactory implements ThreadFactory {
    private static final String PREFIX = "Samza-" + ContainerHeartbeatMonitor.class.getSimpleName() + "-";
    private static final AtomicInteger INSTANCE_NUM = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread t = new Thread(runnable, PREFIX + INSTANCE_NUM.getAndIncrement());
      t.setDaemon(true);
      return t;
    }
  }

  static final class ContainerHeartbeatMetrics {
    private static final String GROUP = "ContainerHeartbeatMonitor";
    private static final String HEARTBEAT_DISCOVERY_TIME_MS = "heartbeat-discovery-time-ms";
    private static final String HEARTBEAT_ESTABLISHED_FAILURE_COUNT = "heartbeat-established-failure-count";
    private static final String HEARTBEAT_ESTABLISHED_WITH_NEW_AM_COUNT = "heartbeat-established-with-new-am-count";
    private static final String HEARTBEAT_EXPIRED_COUNT = "heartbeat-expired-count";

    private final Counter heartbeatEstablishedFailureCount;
    private final Counter heartbeatEstablishedWithNewAmCount;
    private final Counter heartbeatExpiredCount;
    private final Gauge<Long> heartbeatDiscoveryTime;

    public ContainerHeartbeatMetrics(MetricsRegistry registry) {
      heartbeatEstablishedFailureCount = registry.newCounter(GROUP, HEARTBEAT_ESTABLISHED_FAILURE_COUNT);
      heartbeatEstablishedWithNewAmCount = registry.newCounter(GROUP, HEARTBEAT_ESTABLISHED_WITH_NEW_AM_COUNT);
      heartbeatExpiredCount = registry.newCounter(GROUP, HEARTBEAT_EXPIRED_COUNT);
      heartbeatDiscoveryTime = registry.newGauge(GROUP, HEARTBEAT_DISCOVERY_TIME_MS, 0L);
    }

    @VisibleForTesting
    Counter getHeartbeatEstablishedFailureCount() {
      return heartbeatEstablishedFailureCount;
    }

    @VisibleForTesting
    Counter getHeartbeatEstablishedWithNewAmCount() {
      return heartbeatEstablishedWithNewAmCount;
    }

    @VisibleForTesting
    Counter getHeartbeatExpiredCount() {
      return heartbeatExpiredCount;
    }

    private void incrementHeartbeatEstablishedFailureCount() {
      heartbeatEstablishedFailureCount.inc();
    }

    private void incrementHeartbeatEstablishedWithNewAmCount() {
      heartbeatEstablishedWithNewAmCount.inc();
    }

    private void incrementHeartbeatExpiredCount() {
      heartbeatExpiredCount.inc();
    }

    private void setHeartbeatDiscoveryTime(long timeInMillis) {
      heartbeatDiscoveryTime.set(timeInMillis);
    }
  }
}
