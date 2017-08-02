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

package org.apache.samza;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for lease blob operations.
 */
public class LeaseBlobManager {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseBlobManager.class);
  private CloudPageBlob leaseBlob;

  public LeaseBlobManager(CloudPageBlob leaseBlob) {
    this.leaseBlob = leaseBlob;
  }

  /**
   * Acquires a lease on a blob. The lease ID is NULL initially.
   * @param leaseTimeInSec The time in seconds you want to acquire the lease for.
   * @param leaseId Proposed ID you want to acquire the lease with, null if not proposed.
   * @return String that represents lease ID.  Null if acquireLease is unsuccessful because the blob is leased already.
   * @throws AzureException If a Azure storage service error occurred. This includes the case where the blob you're trying to lease does not exist.
   */
  public String acquireLease(int leaseTimeInSec, String leaseId) {
    try {
      String id = leaseBlob.acquireLease(leaseTimeInSec, leaseId);
      LOG.info("Acquired lease with lease id = " + id);
      return id;
    } catch (StorageException storageException) {
      int httpStatusCode = storageException.getHttpStatusCode();
      if (httpStatusCode == HttpStatus.CONFLICT_409) {
        LOG.info("The blob you're trying to acquire is leased already.", storageException);
      } else if (httpStatusCode == HttpStatus.NOT_FOUND_404) {
        LOG.error("The blob you're trying to lease does not exist.", storageException);
        throw new AzureException(storageException);
      } else {
        LOG.error("Error acquiring lease!", storageException);
        throw new AzureException(storageException);
      }
    }
    return null;
  }

  /**
   * Renews the lease on the blob.
   * @param leaseId ID of the lease to be renewed.
   * @return True if lease was renewed successfully, false otherwise.
   */
  public boolean renewLease(String leaseId) {
    try {
      leaseBlob.renewLease(AccessCondition.generateLeaseCondition(leaseId));
      return true;
    } catch (StorageException storageException) {
      LOG.error("Wasn't able to renew lease with lease id: " + leaseId, storageException);
      return false;
    }
  }

  /**
   * Releases the lease on the blob.
   * @param leaseId ID of the lease to be released.
   * @return True if released successfully, false otherwise.
   */
  public boolean releaseLease(String leaseId) {
    try {
      leaseBlob.releaseLease(AccessCondition.generateLeaseCondition(leaseId));
      return true;
    } catch (StorageException storageException) {
      LOG.error("Wasn't able to release lease with lease id: " + leaseId, storageException);
      return false;
    }
  }
  
}