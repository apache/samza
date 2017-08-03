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
import org.apache.samza.ProcessorEntity;
import org.apache.samza.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class invoked by each processor to check if the leader is alive.
 * Checks every 30 seconds.
 * If a processor row hasn't been updated since 30 seconds, the system assumes that the processor has died.
 * All time units are in SECONDS.
 */
public class LeaderLivenessCheckScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderLivenessCheckScheduler.class);
  private static final long CHECK_LIVENESS_DELAY = 30;
  private final ScheduledExecutorService scheduler;
  private final TableUtils table;
  private final AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public LeaderLivenessCheckScheduler(ScheduledExecutorService scheduler, TableUtils table, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.table = table;
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Checking for leader liveness");
        if (!checkIfLeaderAlive()) {
          listener.onStateChange();
        }
      }, CHECK_LIVENESS_DELAY, CHECK_LIVENESS_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  private boolean checkIfLeaderAlive() {
    //Get the leader processor row from the table.
    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(currentJMVersion.get());
    ProcessorEntity leader = null;
    for (ProcessorEntity entity: tableList) {
      if (entity.getIsLeader()) {
        leader = entity;
      }
    }
    // Check if row hasn't been updated since 30 seconds.
    if (System.currentTimeMillis() - leader.getTimestamp().getTime() >= (CHECK_LIVENESS_DELAY * 1000)) {
      return false;
    }
    return true;
  }

}
