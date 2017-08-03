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
import org.apache.samza.BlobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class invoked by each processor to check if the barrier has completed, by checking its state on the blob.
 * Schedules every 15 seconds.
 * All time units are in SECONDS.
 */
public class WorkerBarrierCompleteScheduler implements TaskScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerBarrierCompleteScheduler.class);
  private static final long BARRIER_REACHED_DELAY = 15;
  private final ScheduledExecutorService scheduler;
  private BlobUtils blob;
  private String waitingForState;
  private SchedulerStateChangeListener listener = null;

  public WorkerBarrierCompleteScheduler(ScheduledExecutorService scheduler, BlobUtils blob, String waitingForState) {
    this.scheduler = scheduler;
    this.blob = blob;
    this.waitingForState = waitingForState;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Worker checking for barrier state.");
        String blobState = blob.getBarrierState();
        if (blobState.equals(waitingForState)) {
          listener.onStateChange();
        }
      }, BARRIER_REACHED_DELAY, BARRIER_REACHED_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }
}
