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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.coordinator.CoordinationConstants;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerHeartbeatMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerHeartbeatMonitor.class);
  private static final ThreadFactory THREAD_FACTORY = new HeartbeatThreadFactory();
  private static final CoordinatorStreamValueSerde SERDE = new CoordinatorStreamValueSerde(SetConfig.TYPE);

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
  private String coordinatorUrl;
  private boolean started = false;

  public ContainerHeartbeatMonitor(Runnable onContainerExpired, String coordinatorUrl, String containerExecutionId,
      MetadataStore coordinatorStreamStore, boolean isApplicationMasterHighAvailabilityEnabled, long retryCount,
      long sleepDurationForReconnectWithAM) {
    this(onContainerExpired, new ContainerHeartbeatClient(coordinatorUrl, containerExecutionId),
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY), coordinatorUrl, containerExecutionId,
        coordinatorStreamStore, isApplicationMasterHighAvailabilityEnabled, retryCount, sleepDurationForReconnectWithAM);
  }

  @VisibleForTesting
  ContainerHeartbeatMonitor(Runnable onContainerExpired, ContainerHeartbeatClient containerHeartbeatClient,
      ScheduledExecutorService scheduler, String coordinatorUrl, String containerExecutionId,
      MetadataStore coordinatorStreamStore, boolean isApplicationMasterHighAvailabilityEnabled,
      long retryCount, long sleepDurationForReconnectWithAM) {
    this.onContainerExpired = onContainerExpired;
    this.containerHeartbeatClient = containerHeartbeatClient;
    this.scheduler = scheduler;
    this.coordinatorUrl = coordinatorUrl;
    this.containerExecutionId = containerExecutionId;
    this.coordinatorStreamStore = coordinatorStreamStore;
    this.isApplicationMasterHighAvailabilityEnabled = isApplicationMasterHighAvailabilityEnabled;
    this.retryCount = retryCount;
    this.sleepDurationForReconnectWithAM = sleepDurationForReconnectWithAM;
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
        if (isApplicationMasterHighAvailabilityEnabled) {
          LOG.warn("Failed to establish connection with {}. Checking for new AM", coordinatorUrl);
          if (checkAndEstablishConnectionWithNewAM()) {
            return;
          }
        }
        scheduler.schedule(() -> {
          // On timeout of container shutting down, force exit.
          LOG.error("Graceful shutdown timeout expired. Force exiting.");
          ThreadUtil.logThreadDump("Thread dump at heartbeat monitor shutdown timeout.");
          System.exit(1);
        }, SHUTDOWN_TIMOUT_MS, TimeUnit.MILLISECONDS);
        onContainerExpired.run();
      }
    }, 0, SCHEDULE_MS, TimeUnit.MILLISECONDS);
    started = true;
  }

  public void stop() {
    if (started) {
      LOG.info("Stopping ContainerHeartbeatMonitor");
      scheduler.shutdown();
    }
  }

  private boolean checkAndEstablishConnectionWithNewAM() {
    boolean response = false;
    int attempt = 1;

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
        Thread.currentThread().interrupt();
      }
      attempt++;
    }
    return response;
  }

  @VisibleForTesting
  ContainerHeartbeatClient createContainerHeartbeatClient(String coordinatorUrl, String containerExecutionId) {
    return new ContainerHeartbeatClient(coordinatorUrl, containerExecutionId);
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
}
