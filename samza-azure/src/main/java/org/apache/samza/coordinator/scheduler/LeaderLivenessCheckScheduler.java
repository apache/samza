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
import org.apache.samza.util.BlobUtils;
import org.apache.samza.coordinator.data.ProcessorEntity;
import org.apache.samza.util.TableUtils;
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
  private static final long LIVENESS_CHECK_DELAY_SEC = 10;
  private static final long LIVENESS_DEBOUNCE_TIME_SEC = 30;
  private static final ThreadFactory
      PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("LeaderLivenessCheckScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(PROCESSOR_THREAD_FACTORY);
  private final TableUtils table;
  private final AtomicReference<String> currentJMVersion;
  private final BlobUtils blob;
  private final Consumer<String> errorHandler;
  private final String initialState;
  private SchedulerStateChangeListener listener = null;

  public LeaderLivenessCheckScheduler(Consumer<String> errorHandler, TableUtils table, BlobUtils blob, AtomicReference<String> currentJMVersion, String initialState) {
    this.table = table;
    this.blob = blob;
    this.currentJMVersion = currentJMVersion;
    this.initialState = initialState;
    this.errorHandler = errorHandler;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        try {
          LOG.info("Checking for leader liveness");
          if (!checkIfLeaderAlive()) {
            listener.onStateChange();
          }
        } catch (Exception e) {
          errorHandler.accept("Exception in Leader Liveness Check Scheduler. Stopping the processor...");
        }
      }, LIVENESS_CHECK_DELAY_SEC, LIVENESS_CHECK_DELAY_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  private boolean checkIfLeaderAlive() {
    String currJMV = currentJMVersion.get();
    String blobJMV = blob.getJobModelVersion();
    //Get the leader processor row from the table.
    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(currJMV);
    ProcessorEntity leader = null, nextLeader = null;
    for (ProcessorEntity entity: tableList) {
      if (entity.getIsLeader()) {
        leader = entity;
      }
    }
    int currJMVInt = 0;
    if (!currJMV.equals(initialState)) {
      currJMVInt = Integer.valueOf(currJMV);
    }
    if (Integer.valueOf(blobJMV) > currJMVInt) {
      LOG.info("Leader info 2 in LeaderLivenessCheckScheduker: {}", leader);
      for (ProcessorEntity entity : table.getEntitiesWithPartition(blobJMV)) {
        if (entity.getIsLeader()) {
          nextLeader = entity;
        }
      }
    }
    // Check if row hasn't been updated since 30 seconds.
    if ((leader == null || (System.currentTimeMillis() - leader.getTimestamp().getTime() >= (
        LIVENESS_DEBOUNCE_TIME_SEC * 1000))) && nextLeader == null) {
      return false;
    }
    return true;
  }

  @Override
  public void shutdown() {
    scheduler.shutdownNow();
  }
}