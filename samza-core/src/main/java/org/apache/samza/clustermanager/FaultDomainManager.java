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

import java.util.Map;
import java.util.Set;

/**
 * This interface gets fault domain information of different nodes from the cluster manager (Yarn/Kubernetes/etc.).
 *  It also provides other functionality like exposing all the available fault domains, checking if two hosts belong to
 *  the same fault domain, and getting the valid fault domains that a standby container can be placed on.
 */
public interface FaultDomainManager {

  /**
   * This method returns all the fault domain values in a cluster for RUNNING nodes.
   * @return a set of {@link FaultDomain}s
   */
  Set<FaultDomain> getAllFaultDomains();

  /**
   * This method returns the fault domain a particular node resides on.
   * @param host the host
   * @return the {@link FaultDomain}
   */
  FaultDomain getFaultDomainOfNode(String host);

  /**
   * This method checks if the two hostnames provided reside on the same fault domain.
   * @param host1 hostname
   * @param host2 hostname
   * @return true if the hosts exist on the same fault domain
   */
  boolean checkHostsOnSameFaultDomain(String host1, String host2);

  /**
   * This method gets the set of fault domains that the given active container's corresponding standby can be placed on.
   * @param host The hostname of the active container
   * @return the set of fault domains on which this active container's standby can be scheduled
   */
  Set<FaultDomain> getAllowedFaultDomainsForSchedulingContainer(String host);

  /**
   * This method returns the cached map of nodes to fault domains.
   * @return stored map of node to the fault domain it resides on
   */
  Map<String, FaultDomain> getNodeToFaultDomainMap();

  /**
   * This method computes the node to fault domain map from the cluster resource manager.
   * @return map of node to the fault domain it resides on
   */
  Map<String, FaultDomain> computeNodeToFaultDomainMap();

}
