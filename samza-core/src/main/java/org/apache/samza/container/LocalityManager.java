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

package org.apache.samza.container;

import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;


/**
 * Locality Manager is used to persist and read the container-to-host
 * assignment information from the coordinator stream
 * */
public class LocalityManager {
  private static final String CONTAINER_PREFIX = "SamzaContainer-";
  private static final Logger LOG = LoggerFactory.getLogger(LocalityManager.class);

  private final CoordinatorStreamManager coordinatorStreamManager;
  private final TaskAssignmentManager taskAssignmentManager;

  private Map<String, Map<String, String>> containerToHostMapping = new HashMap<>();

  /**
   * Constructor that creates a read-write or write-only locality manager.
   *
   * @param coordinatorStreamManager Coordinator stream manager.
   */
  public LocalityManager(CoordinatorStreamManager coordinatorStreamManager) {
    this.coordinatorStreamManager = coordinatorStreamManager;
    this.taskAssignmentManager = new TaskAssignmentManager(coordinatorStreamManager);
  }

  /**
   * Method to allow read container locality information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of containerId: (hostname, jmxAddress, jmxTunnelAddress)
   */
  public Map<String, Map<String, String>> readContainerLocality() {
    if (coordinatorStreamManager == null) {
      throw new IllegalStateException("No coordinator stream manager to read container locality from.");
    }

    Map<String, Map<String, String>> allMappings = new HashMap<>();
    for (CoordinatorStreamMessage message : coordinatorStreamManager.getBootstrappedStream(
        SetContainerHostMapping.TYPE)) {
      SetContainerHostMapping mapping = new SetContainerHostMapping(message);
      Map<String, String> localityMappings = new HashMap<>();
      localityMappings.put(SetContainerHostMapping.HOST_KEY, mapping.getHostLocality());
      localityMappings.put(SetContainerHostMapping.JMX_URL_KEY, mapping.getJmxUrl());
      localityMappings.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, mapping.getJmxTunnelingUrl());
      allMappings.put(mapping.getKey(), localityMappings);
    }
    containerToHostMapping = Collections.unmodifiableMap(allMappings);

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Map<String, String>> entry : containerToHostMapping.entrySet()) {
        LOG.debug(String.format("Locality for container %s: %s", entry.getKey(), entry.getValue()));
      }
    }

    return allMappings;
  }

  /**
   * Method to write locality info to coordinator stream. This method is used in {@link SamzaContainer}.
   *
   * @param containerId  the {@link SamzaContainer} ID
   * @param hostName  the hostname
   * @param jmxAddress  the JMX URL address
   * @param jmxTunnelingAddress  the JMX Tunnel URL address
   */
  public void writeContainerToHostMapping(String containerId, String hostName, String jmxAddress,
      String jmxTunnelingAddress) {
    if (coordinatorStreamManager == null) {
      throw new IllegalStateException("No coordinator stream manager to write locality info to.");
    }

    Map<String, String> existingMappings = containerToHostMapping.get(containerId);
    String existingHostMapping =
        existingMappings != null ? existingMappings.get(SetContainerHostMapping.HOST_KEY) : null;
    if (existingHostMapping != null && !existingHostMapping.equals(hostName)) {
      LOG.info("Container {} moved from {} to {}", new Object[]{containerId, existingHostMapping, hostName});
    } else {
      LOG.info("Container {} started at {}", containerId, hostName);
    }
    coordinatorStreamManager.send(
        new SetContainerHostMapping(CONTAINER_PREFIX + containerId, String.valueOf(containerId), hostName, jmxAddress,
            jmxTunnelingAddress));
    Map<String, String> mappings = new HashMap<>();
    mappings.put(SetContainerHostMapping.HOST_KEY, hostName);
    mappings.put(SetContainerHostMapping.JMX_URL_KEY, jmxAddress);
    mappings.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, jmxTunnelingAddress);
    containerToHostMapping.put(containerId, mappings);
  }

  public TaskAssignmentManager getTaskAssignmentManager() {
    return taskAssignmentManager;
  }

  public CoordinatorStreamManager getCoordinatorStreamManager() {
    return coordinatorStreamManager;
  }
}
