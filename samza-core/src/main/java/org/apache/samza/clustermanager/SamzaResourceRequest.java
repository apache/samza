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

package org.apache.samza.clustermanager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specification of a Request for resources from a ClusterResourceManager. A
 * resource request currently includes cpu cores and memory in MB. A preferred host
 * can also be specified with a request.
 *
 * When used with a ordered data structures (for example, priority queues)
 * ordering between two SamzaResourceRequests is defined by their timestamp.
 *
 * //TODO: Define a SamzaResourceRequestBuilder API as specified in SAMZA-881
 */
public class SamzaResourceRequest implements Comparable<SamzaResourceRequest> {
  private static final Logger log = LoggerFactory.getLogger(SamzaResourceRequest.class);

  /**
   * Specifications of a resource request.
   */
  private final int numCores;

  private final int memoryMB;
  /**
   * The preferred host on which the resource must be allocated. Can be set to
   * ContainerRequestState.ANY_HOST if there are no host preferences
   */
  private final String preferredHost;
  /**
   * The set of fault domains on which the resource must be allocated.
   */
  private final Set<FaultDomain> faultDomains;
  /**
   * A request is identified by an unique identifier.
   */
  private final String requestId;
  /**
   * The ID of the Samza Processor which this request is for.
   */
  private final String processorId;

  /**
   * The timestamp in millis when the request was created.
   */
  private final Instant requestTimestamp;

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String processorId) {
    this(numCores, memoryMB, preferredHost, processorId, Instant.now(), ImmutableSet.of());
  }

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String processorId, Set<FaultDomain> faultDomains) {
    this(numCores, memoryMB, preferredHost, processorId, Instant.now(), faultDomains);
  }

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String processorId, Instant requestTimestamp) {
    this.numCores = numCores;
    this.memoryMB = memoryMB;
    this.preferredHost = preferredHost;
    this.requestId = UUID.randomUUID().toString();
    this.processorId = processorId;
    this.requestTimestamp = requestTimestamp;
    this.faultDomains = new HashSet<>();
    log.info("SamzaResourceRequest created for Processor ID: {} on host: {} at time: {} with Request ID: {}, and the following list of fault domains: {}",
            this.processorId, this.preferredHost, this.requestTimestamp, this.requestId, this.faultDomains);
  }

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String processorId, Instant requestTimestamp, Set<FaultDomain> faultDomains) {
    Preconditions.checkNotNull(faultDomains, "Set of fault domains should not be null.");
    this.numCores = numCores;
    this.memoryMB = memoryMB;
    this.preferredHost = preferredHost;
    this.requestId = UUID.randomUUID().toString();
    this.processorId = processorId;
    this.requestTimestamp = requestTimestamp;
    this.faultDomains = faultDomains;
    log.info("SamzaResourceRequest created for Processor ID: {} on host: {} at time: {} with Request ID: {} and the following list of fault domains: {}",
            this.processorId, this.preferredHost, this.requestTimestamp, this.requestId, this.faultDomains.toString());
  }

  public String getProcessorId() {
    return processorId;
  }

  public Instant getRequestTimestamp() {
    return requestTimestamp;
  }

  public String getRequestId() {
    return requestId;
  }

  public int getNumCores() {
    return numCores;
  }

  public String getPreferredHost() {
    return preferredHost;
  }

  public Set<FaultDomain> getFaultDomains() {
    return faultDomains;
  }

  public int getMemoryMB() {
    return memoryMB;
  }

  @Override
  public String toString() {
    return "SamzaResourceRequest{" +
            "numCores=" + numCores +
            ", memoryMB=" + memoryMB +
            ", preferredHost='" + preferredHost + '\'' +
            ", requestId='" + requestId + '\'' +
            ", processorId=" + processorId +
            ", requestTimestampMs=" + requestTimestamp +
            ", faultDomains=" + faultDomains.toString() +
            '}';
  }

  /**
   * Requests are ordered by the processor type and the time at which they were created.
   * Requests with timestamps in the future for retries take less precedence than timestamps in the past or current.
   * Otherwise, active processors take precedence over standby processors, regardless of timestamp.
   * @param o the other
   */
  @Override
  public int compareTo(SamzaResourceRequest o) {
    if (!StandbyTaskUtil.isStandbyContainer(processorId) && StandbyTaskUtil.isStandbyContainer(o.processorId)) {
      return -1;
    }

    if (StandbyTaskUtil.isStandbyContainer(processorId) && !StandbyTaskUtil.isStandbyContainer(o.processorId)) {
      return 1;
    }

    if (requestTimestamp.isBefore(o.requestTimestamp)) {
      return -1;
    }

    if (requestTimestamp.isAfter(o.requestTimestamp)) {
      return 1;
    }

    return 0;
  }
}
