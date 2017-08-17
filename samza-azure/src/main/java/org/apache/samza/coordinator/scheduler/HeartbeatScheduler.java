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

package org.apache.samza.coordinator.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.samza.util.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler invoked by each processor for heartbeating to a row of the table.
 * Heartbeats every 5 seconds.
 * The row is determined by the job model version and processor id passed to the scheduler.
 * All time units are in SECONDS.
 */
public class HeartbeatScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatScheduler.class);
  private static final long HEARTBEAT_DELAY_SEC = 5;
  private static final ThreadFactory PROCESSOR_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("HeartbeatScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(PROCESSOR_THREAD_FACTORY);
  private final String processorId;
  private final TableUtils table;
  private final AtomicReference<String> currentJMVersion;
  private final Consumer<String> errorHandler;

  public HeartbeatScheduler(Consumer<String> errorHandler, TableUtils table, AtomicReference<String> currentJMVersion, final String pid) {
    this.table = table;
    this.currentJMVersion = currentJMVersion;
    processorId = pid;
    this.errorHandler = errorHandler;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        try {
          LOG.info("Updating heartbeat for processor ID: " + processorId + " and job model version: " + currentJMVersion.get());
          table.updateHeartbeat(currentJMVersion.get(), processorId);
        } catch (Exception e) {
          errorHandler.accept("Exception in Heartbeat Scheduler. Stopping the processor...");
        }
      }, HEARTBEAT_DELAY_SEC, HEARTBEAT_DELAY_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {}

  @Override
  public void shutdown() {
    scheduler.shutdownNow();
  }
}