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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encapsulates metadata concerning the failover of an active container.
 */
public class ContainerFailoverState {

  private static final Logger log = LoggerFactory.getLogger(ContainerFailoverState.class);

  // Map of active containers that are in failover, indexed by the active container's resourceID (at the time of failure)
  public final Map<String, FailoverMetadata> failovers;

  public ContainerFailoverState() {
    this.failovers = new ConcurrentHashMap<>();
  }

  /**
   * Register a new failover that has been initiated for the active container resource (identified by its resource ID).
   */
  public void initiatedFailover(String activeContainerID, String activeContainerResourceID,
      String selectedStandbyContainerResourceID, String standbyContainerHost) {
    if (failovers.containsKey(activeContainerResourceID)) {
      FailoverMetadata failoverMetadata = failovers.get(activeContainerResourceID);
      failoverMetadata.updateStandbyContainer(selectedStandbyContainerResourceID, standbyContainerHost);
    } else {
      FailoverMetadata failoverMetadata =
          new FailoverMetadata(activeContainerID, activeContainerResourceID, selectedStandbyContainerResourceID,
              standbyContainerHost);
      this.failovers.put(activeContainerResourceID, failoverMetadata);
    }
  }

  // Check if this standbyContainerResource is present in the failoverState for an active container.
  // This is used to determine if we requested a stop a container.
  public Optional<FailoverMetadata> checkIfUsedForFailover(String standbyContainerResourceId) {

    if (standbyContainerResourceId == null) {
      return Optional.empty();
    }

    for (FailoverMetadata failoverMetadata : failovers.values()) {
      if (failoverMetadata.isStandbyResourceUsed(standbyContainerResourceId)) {
        log.info("Standby container with resource id {} was selected for failover of active container {}",
            standbyContainerResourceId, failoverMetadata.activeContainerID);
        return Optional.of(failoverMetadata);
      }
    }
    return Optional.empty();
  }

  // Check if this activeContainerResource has failover-metadata associated with it
  public Optional<FailoverMetadata> getFailoverMetadata(String activeContainerResourceID) {
    return this.failovers.containsKey(activeContainerResourceID) ? Optional.of(this.failovers.get(activeContainerResourceID)) : Optional
        .empty();
  }

  @Override
  public String toString() {
    return this.failovers.toString();
  }

  public class FailoverMetadata {
    public final String activeContainerID;
    public final String activeContainerResourceID;

    // Map of samza-container-resource ID to host, for each standby container selected for failover of the activeContainer
    public final Map<String, String> selectedStandbyContainers;

    public FailoverMetadata(String activeContainerID, String activeContainerResourceID,
        String selectedStandbyContainerResourceID, String standbyContainerHost) {
      this.activeContainerID = activeContainerID;
      this.activeContainerResourceID = activeContainerResourceID;
      this.selectedStandbyContainers = new HashMap<>();
      this.selectedStandbyContainers.put(selectedStandbyContainerResourceID, standbyContainerHost);
    }

    // Check if this standbyContainerResourceID was used in this failover
    public synchronized boolean isStandbyResourceUsed(String standbyContainerResourceID) {
      return this.selectedStandbyContainers.keySet().contains(standbyContainerResourceID);
    }

    // Get the hostname corresponding to the standby resourceID
    public synchronized String getStandbyContainerHostname(String standbyContainerResourceID) {
      return selectedStandbyContainers.get(standbyContainerResourceID);
    }

    // Add the standbyContainer resource to the list of standbyContainers used in this failover
    public synchronized void updateStandbyContainer(String standbyContainerResourceID, String standbyContainerHost) {
      this.selectedStandbyContainers.put(standbyContainerResourceID, standbyContainerHost);
    }

    @Override
    public String toString() {
      return "[activeContainerID: " + this.activeContainerID + " activeContainerResourceID: "
          + this.activeContainerResourceID + " selectedStandbyContainers:" + selectedStandbyContainers + "]";
    }
  }
}
