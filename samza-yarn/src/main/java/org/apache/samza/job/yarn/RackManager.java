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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.clustermanager.FaultDomain;
import org.apache.samza.clustermanager.FaultDomainManager;
import org.apache.samza.clustermanager.FaultDomainType;

public class RackManager implements FaultDomainManager {

  private final Map<String, FaultDomain> nodeToRackMap;

  public RackManager() {
        this.nodeToRackMap = computeNodeToFaultDomainMap();
    }

  /**
   * This method returns all the rack values in a cluster for RUNNING nodes.
   * @return a set of {@link FaultDomain}s
   */
  @Override
  public Set<FaultDomain> getAllFaultDomains() {
    return new HashSet<>(nodeToRackMap.values());
  }

  /**
   * This method returns the rack a particular node resides on.
   * @param host the host
   * @return the {@link FaultDomain}
   */
  @Override
  public FaultDomain getFaultDomainOfNode(String host) {
    return nodeToRackMap.get(host);
  }

  /**
   * This method checks if the two hostnames provided reside on the same rack.
   * @param host1 hostname
   * @param host2 hostname
   * @return true if the hosts exist on the same rack
   */
  @Override
  public boolean checkHostsOnSameFaultDomain(String host1, String host2) {
    return nodeToRackMap.get(host1).equals(nodeToRackMap.get(host2));
  }

  /**
   * This method gets the set of racks that the given active container's corresponding standby can be placed on.
   * @param host The hostname of the active container
   * @return the set of racks on which this active container's standby can be scheduled
   */
  @Override
  public Set<FaultDomain> getAllowedFaultDomainsForSchedulingContainer(String host) {
    FaultDomain activeContainerRack = nodeToRackMap.get(host);
    Set<FaultDomain> standbyRacks = new HashSet<>(nodeToRackMap.values());
    standbyRacks.remove(activeContainerRack);
    return standbyRacks;
  }

  /**
   * This method returns the cached map of nodes to racks.
   * @return stored map of node to the rack it resides on
   */
  @Override
  public Map<String, FaultDomain> getNodeToFaultDomainMap() {
    return nodeToRackMap;
  }

  /**
   * This method gets the node to rack (fault domain for Yarn) mapping from Yarn for all running nodes.
   * @return A map of hostname to rack name.
   */
  @Override
  public Map<String, FaultDomain> computeNodeToFaultDomainMap() {
    YarnClientImpl yarnClient = new YarnClientImpl();
    Map<String, FaultDomain> nodeToRackMap = new HashMap<>();
    try {
      List<NodeReport> nodeReport = yarnClient.getNodeReports(NodeState.RUNNING);
      nodeReport.forEach(report -> {
        FaultDomain rack = new FaultDomain(FaultDomainType.RACK, report.getRackName());
        nodeToRackMap.put(report.getNodeId().getHost(), rack);
      });
    } catch (YarnException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return nodeToRackMap;
  }
}
