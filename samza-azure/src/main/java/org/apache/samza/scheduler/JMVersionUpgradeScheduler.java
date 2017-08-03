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
  private static final long CHECK_UPGRADE_DELAY = 5;
  private final ScheduledExecutorService scheduler;
  private final BlobUtils blob;
  private final AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public JMVersionUpgradeScheduler(ScheduledExecutorService scheduler, BlobUtils blob, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.blob = blob;
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Checking for job model version upgrade");
        // Read job model version from the blob.
        String blobJMV = blob.getJobModelVersion();
        // Check if the job model version on the blob is consistent with the job model version that the processor is operating on.
        if (!currentJMVersion.get().equals(blobJMV)) {
          listener.onStateChange();
        }
      }, CHECK_UPGRADE_DELAY, CHECK_UPGRADE_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

}
