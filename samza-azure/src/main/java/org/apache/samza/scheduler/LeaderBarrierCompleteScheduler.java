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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.samza.ProcessorEntity;
import org.apache.samza.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class invoked by the leader to check if the barrier has completed.
 * Checks every 15 seconds.
 * The leader polls the Azure processor table in order to track this.
 * The barrier is completed if all processors that are listed alive on the blob, have entries in the Azure table with the new job model version.
 * All time units are in SECONDS.
 */
public class LeaderBarrierCompleteScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderBarrierCompleteScheduler.class);
  private static final long BARRIER_REACHED_DELAY_SEC = 5;
  private static final long BARRIER_TIMEOUT_SEC = 30;
  private static final ThreadFactory
      PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("LeaderBarrierCompleteScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(PROCESSOR_THREAD_FACTORY);
  private final TableUtils table;
  private final String nextJMVersion;
  private final Set<String> blobProcessorSet;
  private final long startTime;
  private final AtomicBoolean barrierTimeout;
  private final Consumer<String> errorHandler;
  private final String processorId;
  private final AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public LeaderBarrierCompleteScheduler(Consumer<String> errorHandler, TableUtils table, String nextJMVersion,
      List<String> blobProcessorList, long startTime, AtomicBoolean barrierTimeout, AtomicReference<String> currentJMVersion, final String pid) {
    this.table = table;
    this.nextJMVersion = nextJMVersion;
    this.blobProcessorSet = new HashSet<>(blobProcessorList);
    this.startTime = startTime;
    this.barrierTimeout = barrierTimeout;
    this.errorHandler = errorHandler;
    this.processorId = pid;
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        try {
          if (!table.getEntity(currentJMVersion.get(), processorId).getIsLeader()) {
            LOG.info("Not the leader anymore. Shutting down LeaderBarrierCompleteScheduler.");
            scheduler.shutdownNow();
            barrierTimeout.getAndSet(true);
            listener.onStateChange();
          }
          LOG.info("Leader checking for barrier state");
          // Get processor IDs listed in the table that have the new job model verion.
          Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(nextJMVersion);
          Set<String> tableProcessors = new HashSet<>();
          for (ProcessorEntity entity : tableList) {
            tableProcessors.add(entity.getRowKey());
          }
          LOG.info("blobProcessorSet = {}", blobProcessorSet);
          LOG.info("tableProcessors = {}", tableProcessors);
          if ((System.currentTimeMillis() - startTime) > (BARRIER_TIMEOUT_SEC * 1000)) {
            barrierTimeout.getAndSet(true);
            listener.onStateChange();
          } else if (blobProcessorSet.equals(tableProcessors)) {
            listener.onStateChange();
          }
        } catch (Exception e) {
          errorHandler.accept("Exception in LeaderBarrierCompleteScheduler. Stopping the processor...");
        }
      }, BARRIER_REACHED_DELAY_SEC, BARRIER_REACHED_DELAY_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  @Override
  public void shutdown() {
    scheduler.shutdownNow();
  }
}