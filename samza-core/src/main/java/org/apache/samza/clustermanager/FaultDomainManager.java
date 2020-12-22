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

import java.util.Set;
import org.apache.samza.annotation.InterfaceStability;

/**
 *  This interface gets fault domain information of all hosts that are running in the cluster,
 *  from the cluster manager (Yarn/Kubernetes/etc.).
 *  It also provides other functionality like exposing all the available fault domains, checking if two hosts belong to
 *  the same fault domain, and getting the valid fault domains that a container can be placed on (for ex: based on standby constraints).
 *  The host to fault domain map used here will always be cached and only updated in case the AM dies or an active
 *  container is assigned to a host which is not in the map.
 *  This is not thread-safe.
 */
@InterfaceStability.Unstable
public interface FaultDomainManager {

  /**
   * This method returns all the fault domain values in a cluster, for all hosts that are healthy, up and running.
   * This set might not be up to date with the current state of the cluster, as its freshness is an implementation detail.
   * @return a set of {@link FaultDomain}s
   */
  Set<FaultDomain> getAllFaultDomains();

  /**
   * This method returns the fault domain a particular host resides on based on the internal cache.
   * @param host the host
   * @return the {@link FaultDomain}
   */
  Set<FaultDomain> getFaultDomainsForHost(String host);

  /**
   * This method returns true if the fault domains on which these two hosts reside are exactly the same, false otherwise.
   * @param host1 hostname
   * @param host2 hostname
   * @return true if the hosts exist on the same fault domain
   */
  boolean hasSameFaultDomains(String host1, String host2);

}
