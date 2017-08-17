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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.samza.BlobUtils;
import org.apache.samza.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class invoked by the leader to check for changes in the list of live processors.
 * Checks every 30 seconds.
 * If a processor row hasn't been updated since 30 seconds, the system assumes that the processor has died.
 * All time units are in SECONDS.
 */
public class LivenessCheckScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LivenessCheckScheduler.class);
  private static final long LIVENESS_CHECK_DELAY_SEC = 5;
  private static final ThreadFactory
      PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("LivenessCheckScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(PROCESSOR_THREAD_FACTORY);
  private final TableUtils table;
  private final BlobUtils blob;
  private final AtomicReference<String> currentJMVersion;
  private final AtomicReference<List<String>> liveProcessorsList;
  private final Consumer<String> errorHandler;
  private SchedulerStateChangeListener listener = null;
  private final String processorId;

  public LivenessCheckScheduler(Consumer<String> errorHandler, TableUtils table, BlobUtils blob, AtomicReference<String> currentJMVersion, final String pid) {
    this.table = table;
    this.blob = blob;
    this.currentJMVersion = currentJMVersion;
    liveProcessorsList = new AtomicReference<>(null);
    this.errorHandler = errorHandler;
    this.processorId = pid;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        try {
          if (!table.getEntity(currentJMVersion.get(), processorId).getIsLeader()) {
            LOG.info("Not the leader anymore. Shutting down LivenessCheckScheduler.");
            scheduler.shutdownNow();
          }
          LOG.info("Checking for list of live processors");
          //Get the list of live processors published on the blob.
          Set<String> currProcessors = new HashSet<>(blob.getLiveProcessorList());
          //Get the list of live processors from the table. This is the current system state.
          Set<String> liveProcessors = table.getActiveProcessorsList(currentJMVersion);
          //Invoke listener if the table list is not consistent with the blob list.
          if (!liveProcessors.equals(currProcessors)) {
            liveProcessorsList.getAndSet(new ArrayList<>(liveProcessors));
            listener.onStateChange();
          }
        } catch (Exception e) {
          errorHandler.accept("Exception in Liveness Check Scheduler. Stopping the processor...");
        }
      }, LIVENESS_CHECK_DELAY_SEC, LIVENESS_CHECK_DELAY_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  public AtomicReference<List<String>> getLiveProcessors() {
    return liveProcessorsList;
  }

  @Override
  public void shutdown() {
    scheduler.shutdownNow();
  }
}