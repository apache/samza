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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.samza.BarrierState;
import org.apache.samza.BlobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler invoked by each processor to check for job model version upgrades on the blob.
 * Checks every 5 seconds.
 * The processor polls the leader blob in order to track this.
 * All time units are in SECONDS.
 */
public class JMVersionUpgradeScheduler implements TaskScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(JMVersionUpgradeScheduler.class);
  private static final long JMV_UPGRADE_DELAY_SEC = 5;
  private static final ThreadFactory
      PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("JMVersionUpgradeScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(PROCESSOR_THREAD_FACTORY);
  private final BlobUtils blob;
  private final AtomicReference<String> currentJMVersion;
  private final AtomicBoolean versionUpgradeDetected;
  private final String processorId;
  private final Consumer<String> errorHandler;
  private SchedulerStateChangeListener listener = null;

  public JMVersionUpgradeScheduler(Consumer<String> errorHandler, BlobUtils blob,
      AtomicReference<String> currentJMVersion, AtomicBoolean versionUpgradeDetected, String processorId) {
    this.blob = blob;
    this.currentJMVersion = currentJMVersion;
    this.versionUpgradeDetected = versionUpgradeDetected;
    this.processorId = processorId;
    this.errorHandler = errorHandler;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        try {
          LOG.info("Checking for job model version upgrade");
          // Read job model version from the blob.
          String blobJMV = blob.getJobModelVersion();
          LOG.info("Blob JMV: {}", blobJMV);
          String blobBarrierState = blob.getBarrierState();
          String currentJMV = currentJMVersion.get();
          LOG.info("currentJMV: {}", currentJMV);
          String expectedBarrierState = BarrierState.START.toString() + " " + blobJMV;
          List<String> processorList = blob.getLiveProcessorList();
          // Check if the job model version on the blob is consistent with the job model version that the processor is operating on.
          if (processorList != null && processorList.contains(processorId) && !currentJMV.equals(blobJMV) && blobBarrierState.equals(expectedBarrierState) && !versionUpgradeDetected.get()) {
            listener.onStateChange();
          }
        } catch (Exception e) {
          errorHandler.accept("Exception in Job Model Version Upgrade Scheduler. Stopping the processor...");
        }
      }, JMV_UPGRADE_DELAY_SEC, JMV_UPGRADE_DELAY_SEC, TimeUnit.SECONDS);
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