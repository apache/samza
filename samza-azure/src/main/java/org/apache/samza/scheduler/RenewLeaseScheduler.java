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
import org.apache.samza.LeaseBlobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler class to keep renewing the lease once an entity has acquired it.
 * Renews every 45 seconds.
 * All time units are in SECONDS.
 */
public class RenewLeaseScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RenewLeaseScheduler.class);
  private static final long DELAY_IN_SEC = 45;
  private final ScheduledExecutorService scheduler;
  private final LeaseBlobManager leaseBlobManager;
  private final AtomicReference<String> leaseId;
  private SchedulerStateChangeListener listener = null;

  public RenewLeaseScheduler(ScheduledExecutorService scheduler, LeaseBlobManager leaseBlobManager, AtomicReference<String> leaseId) {
    this.scheduler = scheduler;
    this.leaseBlobManager = leaseBlobManager;
    this.leaseId = leaseId;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Renewing lease");
        boolean status = false;
        while (!status) {
          status = leaseBlobManager.renewLease(leaseId.get());
        }
      }, DELAY_IN_SEC, DELAY_IN_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }
}