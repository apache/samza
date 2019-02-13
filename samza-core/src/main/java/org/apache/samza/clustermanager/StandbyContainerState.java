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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.storage.kv.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encapsulates state concerning standby-containers.
 */
public class StandbyContainerState {

  private static final Logger log = LoggerFactory.getLogger(StandbyContainerState.class);

  // Map of samza containerIDs to their corresponding active and standby containers, e.g., 0 -> {0-0, 0-1}, 0-0 -> {0, 0-1}
  // This is used for checking no two standbys or active-standby-pair are started on the same host
  private final Map<String, List<String>> standbyContainerConstraints;

  // Map of active containers that are in failover, indexed by the active container's resourceID (at the time of failure)
  private final Map<String, FailoverMetadata> failovers;

  public StandbyContainerState(JobModel jobModel) {
    this.failovers = new ConcurrentHashMap<>();
    this.standbyContainerConstraints = new HashMap<>();

    // populate the standbyContainerConstraints map by iterating over all containers
    jobModel.getContainers().keySet().forEach(containerId -> standbyContainerConstraints.put(containerId,
            StandbyTaskUtil.getStandbyContainerConstraints(containerId, jobModel)));
    log.info("Populated standbyContainerConstraints map {}", standbyContainerConstraints);
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

  /**
   * Check if this standbyContainerResource is present in the failoverState for an active container.
   * This is used to determine if we requested a stop a container.
   */
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

  /**
   * Check if matching this SamzaResourceRequest to the given resource, meets all standby-container container constraints.
   *
   * @param request The resource request to match.
   * @param samzaResource The samzaResource to potentially match the resource to.
   * @param runningContainers The set of currently running containers.
   * @param pendingContainers The set of containers that are currently in pending state.
   * @return
   */
  public boolean checkStandbyConstraints(SamzaResourceRequest request, SamzaResource samzaResource,
      Map<String, SamzaResource> runningContainers, Map<String, SamzaResource> pendingContainers) {
    String containerIDToStart = request.getContainerID();
    String host = samzaResource.getHost();
    List<String> containerIDsForStandbyConstraints = this.standbyContainerConstraints.get(containerIDToStart);

    // Check if any of these conflicting containers are running/launching on host
    for (String containerID : containerIDsForStandbyConstraints) {
      SamzaResource resource = pendingContainers.get(containerID);

      // return false if a conflicting container is pending for launch on the host
      if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already scheduled on this host",
            containerIDToStart, samzaResource.getHost(), containerID);
        return false;
      }

      // return false if a conflicting container is running on the host
      resource = runningContainers.get(containerID);
      if (resource != null && resource.getHost().equals(host)) {
        log.info("Container {} cannot be started on host {} because container {} is already running on this host",
            containerIDToStart, samzaResource.getHost(), containerID);
        return false;
      }
    }

    return true;
  }

  /**
   * Helper method to select a standby for a given activeContainerID
   * @param activeContainerID Samza containerID of the active container
   * @param activeContainerResourceID ResourceID of the active container at the time of its last failure
   * @param state SamzaApplication State object
   * @return
   */
  public Optional<Entry<String, SamzaResource>> selectStandby(String activeContainerID, String activeContainerResourceID, SamzaApplicationState state) {

    log.info("Standby containers {} for active container {}", this.standbyContainerConstraints.get(activeContainerID), activeContainerID);

    // obtain any existing failover metadata
    Optional<StandbyContainerState.FailoverMetadata> failoverMetadata =
        activeContainerResourceID == null ? Optional.empty() : this.getFailoverMetadata(activeContainerResourceID);

    // Iterate over the list of running standby containers, to find a standby resource that we have not already
    // used for a failover for this active resoruce
    for (String standbyContainerID : this.standbyContainerConstraints.get(activeContainerID)) {
      if (state.runningContainers.containsKey(standbyContainerID)) {
        SamzaResource standbyContainerResource = state.runningContainers.get(standbyContainerID);

        // use this standby if there was no previous failover or if this standbyResource was not used for it
        if (!failoverMetadata.isPresent() || !failoverMetadata.get().isStandbyResourceUsed(standbyContainerResource.getResourceID())) {

          log.info("Returning standby container {} in running state for active container {}", standbyContainerID,
              activeContainerID);
          return Optional.of(new Entry<>(standbyContainerID, standbyContainerResource));
        }
      }
    }

    log.info("Did not find any running standby container for active container {}", activeContainerID);
    return Optional.empty();
  }

  /**
   * Check if this activeContainerResource has failover-metadata associated with it
   */
  public Optional<FailoverMetadata> getFailoverMetadata(String activeContainerResourceID) {
    return this.failovers.containsKey(activeContainerResourceID) ? Optional.of(
        this.failovers.get(activeContainerResourceID)) : Optional.empty();
  }

  @Override
  public String toString() {
    return this.failovers.toString();
  }

  /**
   * Encapsulates metadata concerning the failover of an active container.
   */
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
