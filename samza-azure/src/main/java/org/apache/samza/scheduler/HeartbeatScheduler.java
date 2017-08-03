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

package org.apache.samza.scheduler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.TableUtils;
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
  private static final long HEARTBEAT_DELAY = 5;
  private final ScheduledExecutorService scheduler;
  private final String processorId;
  private final TableUtils table;
  private final AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public HeartbeatScheduler(ScheduledExecutorService scheduler, TableUtils table, AtomicReference<String> currentJMVersion, final String pid) {
    this.scheduler = scheduler;
    this.table = table;
    this.currentJMVersion = currentJMVersion;
    processorId = pid;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Updating heartbeat");
        table.updateHeartbeat(currentJMVersion.get(), processorId);
      }, HEARTBEAT_DELAY, HEARTBEAT_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }
}
