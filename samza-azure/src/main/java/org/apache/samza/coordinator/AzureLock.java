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

package org.apache.samza.coordinator;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.AzureException;
import org.apache.samza.util.BlobUtils;
import org.apache.samza.util.LeaseBlobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Distributed lock primitive for Azure.
 */
public class AzureLock implements DistributedLock {

  private static final Logger LOG = LoggerFactory.getLogger(AzureLock.class);
  private static final int LEASE_TIME_IN_SEC = 60;
  private AtomicBoolean hasLock;
  private AtomicReference<String> leaseId;
  private final LeaseBlobManager leaseBlobManager;

  public AzureLock(BlobUtils blobUtils) {
    this.hasLock = new AtomicBoolean(false);
    leaseBlobManager = new LeaseBlobManager(blobUtils.getBlob());
    this.leaseId = new AtomicReference<>(null);
  }

  /**
   * Tries to acquire a lock in order to create intermediate streams. On failure to acquire lock, it keeps trying until the lock times out.
   * The lock is acquired when the blob is leased successfully.
   * @param timeout Duration after which timeout occurs.
   * @param unit Time Unit of the timeout defined above.
   * @return true if the lock was acquired successfully, false if lock acquire operation is unsuccessful even after subsequent tries within the timeout range.
   */
  @Override
  public boolean lock(long timeout, TimeUnit unit) {
    //Start timer for timeout
    long startTime = System.currentTimeMillis();
    long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
    Random random = new Random();

    while ((System.currentTimeMillis() - startTime) < lockTimeout) {
      try {
        leaseId.getAndSet(leaseBlobManager.acquireLease(LEASE_TIME_IN_SEC, leaseId.get()));
      } catch (AzureException e) {
        return false;
      }
      if (leaseId.get() != null) {
        LOG.info("Acquired lock!");
        hasLock.getAndSet(true);
        return true;
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        LOG.info("Trying to acquire lock again...");
      }
    }
    return false;
  }

  /**
   * Unlocks, by releasing the lease on the blob.
   */
  @Override
  public void unlock() {
    boolean status = leaseBlobManager.releaseLease(leaseId.get());
    if (status) {
      LOG.info("Unlocked successfully.");
      hasLock.getAndSet(false);
      leaseId.getAndSet(null);
    } else {
      LOG.info("Unable to unlock.");
    }
  }
}