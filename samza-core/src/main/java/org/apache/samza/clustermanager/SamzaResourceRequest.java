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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Specification of a Request for resources from a ClusterResourceManager. A
 * resource request currently includes cpu cores and memory in MB. A preferred host
 * can also be specified with a request.
 *
 * When used with an ordered data structures (for example, priority queues)
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
   * A request is identified by an unique identifier.
   */
  private final String requestId;
  /**
   * The ID of the StreamProcessor which this request is for.
   */
  private final String containerId;

  /**
   * The timestamp in millis when the request was created.
   */
  private final long requestTimestampMs;

  public SamzaResourceRequest(int numCores, int memoryMB, String preferredHost, String expectedContainerID) {
    this.numCores = numCores;
    this.memoryMB = memoryMB;
    this.preferredHost = preferredHost;
    this.requestId = UUID.randomUUID().toString();
    this.containerId = expectedContainerID;
    this.requestTimestampMs = System.currentTimeMillis();
    log.info("Resource Request created for {} on {} at {}", new Object[] {this.containerId, this.preferredHost, this.requestTimestampMs});
  }

  public String getContainerId() {
    return containerId;
  }

  public long getRequestTimestampMs() {
    return requestTimestampMs;
  }

  public String getRequestID() {
    return requestId;
  }

  public int getNumCores() {
    return numCores;
  }

  public String getPreferredHost() {
    return preferredHost;
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
            ", requestID='" + requestId + '\'' +
            ", containerID=" + containerId +
            ", requestTimestampMs=" + requestTimestampMs +
            '}';
  }

  /**
   * Requests are ordered by the time at which they were created.
   * @param o the other
   */
  @Override
  public int compareTo(SamzaResourceRequest o) {
    if (this.requestTimestampMs < o.requestTimestampMs)
      return -1;
    if (this.requestTimestampMs > o.requestTimestampMs)
      return 1;
    return 0;
  }
}
