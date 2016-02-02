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

package org.apache.samza.job.yarn;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;

/**
 * SamzaContainerRequest encapsulate the ContainerRequest and the meta-information of the request.
 */
public class SamzaContainerRequest implements Comparable<SamzaContainerRequest> {
  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  private Priority priority;
  private Resource capability;

  /**
   *  If host-affinity is enabled, the request specifies a preferred host for the container
   *  If not, preferredHost defaults to ANY_HOST
   */
  private String preferredHost;
  // Timestamp at which the request is issued. Used to check request expiry
  private Long requestTimestamp;
  // Actual Container Request that is issued to the RM
  public AMRMClient.ContainerRequest issuedRequest;
  // Container Id that is expected to run on the container returned for this request
  public int expectedContainerId;

  public SamzaContainerRequest(int memoryMb, int cpuCores, int priority, int expectedContainerId, String preferredHost) {
    this.capability = Resource.newInstance(memoryMb, cpuCores);
    this.priority = Priority.newInstance(priority);
    this.expectedContainerId = expectedContainerId;
    if (preferredHost == null) {
      this.preferredHost = ANY_HOST;
      this.issuedRequest = new AMRMClient.ContainerRequest(capability, null, null, this.priority);
    } else {
      this.preferredHost = preferredHost;
      this.issuedRequest = new AMRMClient.ContainerRequest(
          capability,
          new String[]{this.preferredHost},
          null,
          this.priority);
    }

    this.requestTimestamp = System.currentTimeMillis();
  }

  // Convenience class for unit testing
  public SamzaContainerRequest(int expectedContainerId, String preferredHost) {
    this(
        AbstractContainerAllocator.DEFAULT_CONTAINER_MEM,
        AbstractContainerAllocator.DEFAULT_CPU_CORES,
        AbstractContainerAllocator.DEFAULT_PRIORITY,
        expectedContainerId,
        preferredHost);
  }

  public Resource getCapability() {
    return capability;
  }

  public Priority getPriority() {
    return priority;
  }

  public AMRMClient.ContainerRequest getIssuedRequest() {
    return issuedRequest;
  }

  public int getExpectedContainerId() {
    return expectedContainerId;
  }

  public String getPreferredHost() {
    return preferredHost;
  }

  public Long getRequestTimestamp() {
    return requestTimestamp;
  }

  @Override
  public String toString() {
    return "[requestIssueTime=" + issuedRequest.toString() + "]";
  }

  @Override
  public int compareTo(SamzaContainerRequest o) {
    if(requestTimestamp < o.requestTimestamp)
      return -1;
    if(requestTimestamp > o.requestTimestamp)
      return 1;
    return 0;
  }
}
