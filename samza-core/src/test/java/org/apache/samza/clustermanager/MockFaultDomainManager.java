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

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockFaultDomainManager implements FaultDomainManager {

  private final Map<String, FaultDomain> nodeToFaultDomainMap;

  public MockFaultDomainManager() {
    FaultDomain faultDomain1 = new FaultDomain(FaultDomainType.RACK, "rack-1");
    FaultDomain faultDomain2 = new FaultDomain(FaultDomainType.RACK, "rack-2");
    FaultDomain faultDomain3 = new FaultDomain(FaultDomainType.RACK, "rack-1");
    FaultDomain faultDomain4 = new FaultDomain(FaultDomainType.RACK, "rack-3");
    FaultDomain faultDomain5 = new FaultDomain(FaultDomainType.RACK, "rack-4");
    nodeToFaultDomainMap = ImmutableMap.of("host-1", faultDomain1, "host-2", faultDomain2,
            "host-3", faultDomain3, "host-4", faultDomain4, "host-5", faultDomain5);
  }

  @Override
  public Set<FaultDomain> getAllFaultDomains() {
    return new HashSet<>(nodeToFaultDomainMap.values());
  }

  @Override
  public FaultDomain getFaultDomainOfNode(String host) {
    return nodeToFaultDomainMap.get(host);
  }

  @Override
  public boolean checkHostsOnSameFaultDomain(String host1, String host2) {
    return nodeToFaultDomainMap.get(host1).equals(nodeToFaultDomainMap.get(host2));
  }

  @Override
  public Set<FaultDomain> getAllowedFaultDomainsForSchedulingContainer(String host) {
    FaultDomain activeContainerRack = nodeToFaultDomainMap.get(host);
    Set<FaultDomain> standbyRacks = new HashSet<>(nodeToFaultDomainMap.values());
    standbyRacks.remove(activeContainerRack);
    return standbyRacks;
  }

  @Override
  public Map<String, FaultDomain> getNodeToFaultDomainMap() {
    return nodeToFaultDomainMap;
  }

  @Override
  public Map<String, FaultDomain> computeNodeToFaultDomainMap() {
    return nodeToFaultDomainMap;
  }
}
