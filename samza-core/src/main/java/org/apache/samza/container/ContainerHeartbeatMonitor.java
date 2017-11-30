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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContainerHeartbeatMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerHeartbeatMonitor.class);
  private static final ThreadFactory THREAD_FACTORY = new HeartbeatThreadFactory();
  private static final int SCHEDULE_MS = 60000;
  private static final int SHUTDOWN_TIMOUT_MS = 120000;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  private final Runnable onContainerExpired;
  private final ContainerHeartbeatClient containerHeartbeatClient;
  private boolean started = false;

  public ContainerHeartbeatMonitor(Runnable onContainerExpired, ContainerHeartbeatClient containerHeartbeatClient) {
    this.onContainerExpired = onContainerExpired;
    this.containerHeartbeatClient = containerHeartbeatClient;
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
          scheduler.schedule(() -> {
              // On timeout of container shutting down, force exit.
              LOG.error("Gracefully shutdown timeout expired. Force exiting.");
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
