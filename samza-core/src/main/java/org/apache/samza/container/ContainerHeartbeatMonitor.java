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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class ContainerHeartbeatMonitor {
  private static Logger log = LoggerFactory.getLogger(ContainerHeartbeatMonitor.class);
  private static final ThreadFactory THREAD_FACTORY = new HeartbeatThreadFactoryImpl();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  private final Runnable onContainerExpired;
  private final ContainerHeartbeatClient containerHeartbeatClient;
  private final int scheduleMS = 60000;

  public ContainerHeartbeatMonitor(Runnable onContainerExpired, ContainerHeartbeatClient containerHeartbeatClient) {
    this.onContainerExpired = onContainerExpired;
    this.containerHeartbeatClient = containerHeartbeatClient;
  }

  public void start() {
    log.info("Starting ContainerHeartbeatMonitor");
    scheduler.scheduleAtFixedRate(() -> {
        if (!containerHeartbeatClient.isAlive()) {
          onContainerExpired.run();
        }
      }, 0, scheduleMS, MILLISECONDS);
  }

  public void stop() {
    log.info("Stopping ContainerHeartbeatMonitor");
    scheduler.shutdown();
  }

  private static class HeartbeatThreadFactoryImpl implements ThreadFactory {
    private static final String PREFIX = "Samza-" + ContainerHeartbeatMonitor.class.getSimpleName() + "-";
    private static final AtomicInteger INSTANCE_NUM = new AtomicInteger();

    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, PREFIX + INSTANCE_NUM.getAndIncrement());
    }
  }
}
