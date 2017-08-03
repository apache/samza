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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.BlobUtils;
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
  private static final long BARRIER_REACHED_DELAY = 15;
  private final ScheduledExecutorService scheduler;
  private final TableUtils table;
  private final BlobUtils blob;
  private final String nextJMVersion;
  private SchedulerStateChangeListener listener = null;

  public LeaderBarrierCompleteScheduler(ScheduledExecutorService scheduler, TableUtils table, BlobUtils blob, String nextJMVersion) {
    this.scheduler = scheduler;
    this.table = table;
    this.blob = blob;
    this.nextJMVersion = nextJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Leader checking for barrier state");

        // Get processor IDs listed in the table that have the new job model verion.
        Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(nextJMVersion);
        Set<String> tableProcessors = new HashSet<>();
        for (ProcessorEntity entity: tableList) {
          tableProcessors.add(entity.getRowKey());
        }
        // Get list of live processors from the blob.
        Set<String> blobProcessorList = new HashSet<>(blob.getLiveProcessorList());

        //If they match, call the state change listener.
        if (blobProcessorList.equals(tableProcessors)) {
          listener.onStateChange();
        }
      }, BARRIER_REACHED_DELAY, BARRIER_REACHED_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

}
