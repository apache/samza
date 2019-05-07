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

import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.samza.SamzaException;
import org.apache.samza.metadatastore.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generates RunId for Standalone use case
 * If there is only one processor in the quorum (registered with ClusterMembership) then create new runid and add to store
 * Else read runid from the store
 *
 * Steps to generate:
 * 1. acquire lock
 * 2. add self to quorum (register itself with ClusterMembership)
 * 3. get number of processors in quorum
 * 4. if qurorum size is 1 (only self) then create new runid and write to store
 * 5. if quorum size if greater than 1 then read runid from store
 * 6. unlock
 */
public class RunIdGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(RunIdGenerator.class);

  private final CoordinationUtils coordinationUtils;
  private final MetadataStore metadataStore;
  private final ClusterMembership clusterMembership;

  public RunIdGenerator(CoordinationUtils coordinationUtils, MetadataStore metadataStore) {
    Preconditions.checkNotNull(coordinationUtils, "CoordinationUtils cannot be null");
    Preconditions.checkNotNull(metadataStore, "MetadataStore cannot be null");
    this.coordinationUtils = coordinationUtils;
    this.metadataStore = metadataStore;
    this.clusterMembership = coordinationUtils.getClusterMembership();
    if (clusterMembership == null) {
      throw new SamzaException("Failed to create utils for run id generation");
    }
  }

  public Optional<String> getRunId() {
    DistributedLock runIdLock;
    String runId = null;

    runIdLock = coordinationUtils.getLock(CoordinationConstants.RUNID_LOCK_ID);
    if (runIdLock == null) {
      throw new SamzaException("Failed to create utils for run id generation");
    }

    try {
      // acquire lock to write or read run.id
      if (runIdLock.lock(Duration.ofMillis(CoordinationConstants.LOCK_TIMEOUT_MS))) {
        LOG.info("lock acquired for run.id generation by this processor");
        clusterMembership.registerProcessor();
        int numberOfActiveProcessors = clusterMembership.getNumberOfProcessors();
        if (numberOfActiveProcessors == 0) {
          String msg = String.format("Processor failed to fetch number of processors for run.id generation");
          throw new SamzaException(msg);
        }
        if (numberOfActiveProcessors == 1) {
          runId =
              String.valueOf(System.currentTimeMillis()) + "-" + UUID.randomUUID().toString().substring(0, 8);
          LOG.info("Writing the run id for this run as {}", runId);
          metadataStore.put(CoordinationConstants.RUNID_STORE_KEY, runId.getBytes("UTF-8"));
        } else {
          runId = new String(metadataStore.get(CoordinationConstants.RUNID_STORE_KEY));
          LOG.info("Read the run id for this run as {}", runId);
        }
        runIdLock.unlock();
      }
    } catch (TimeoutException e) {
      throw new SamzaException("Processor timed out waiting to acquire lock for run.id generation", e);
    } catch (UnsupportedEncodingException e) {
      throw new SamzaException("Processor could not serialize/deserialize string for run.id generation", e);
    }
    return Optional.ofNullable(runId);
  }

  /**
   * might be called several times and hence should be idempotent
   * that means the clusterMembership.unregisterProcessor should be idempotent
   */
  public void close() {
    if (clusterMembership != null) {
      clusterMembership.unregisterProcessor();
    }
  }
}
