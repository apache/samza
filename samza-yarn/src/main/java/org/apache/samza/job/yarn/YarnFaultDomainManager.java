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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.clustermanager.FaultDomain;
import org.apache.samza.clustermanager.FaultDomainManager;
import org.apache.samza.clustermanager.FaultDomainType;

/**
 * This class functionality works with the assumption that the job.standbytasks.replication.factor is 2.
 * For values greater than 2, it is possible that the standby containers could be on the same rack as the active, or the already existing standby.
 */
public class YarnFaultDomainManager implements FaultDomainManager {

  private final Multimap<String, FaultDomain> hostToRackMap;

  public YarnFaultDomainManager() {
    this.hostToRackMap = computeHostToFaultDomainMap();
  }

  /**
   * This method returns all the last cached rack values in a cluster, for all hosts that are healthy, up and running.
   * @return a set of {@link FaultDomain}s
   */
  @Override
  public Set<FaultDomain> getAllFaultDomains() {
    return new HashSet<>(hostToRackMap.values());
  }

  /**
   * This method returns all the racks a particular host resides on based on the internal cache.
   * @param host the host
   * @return the {@link FaultDomain}
   */
  @Override
  public Set<FaultDomain> getFaultDomainOfHost(String host) {
    return new HashSet<>(hostToRackMap.get(host));
  }

  /**
   * This method checks if the two hostnames provided reside on the same rack.
   * @param host1 hostname
   * @param host2 hostname
   * @return true if the hosts exist on the same rack
   */
  @Override
  public boolean checkHostsOnSameFaultDomain(String host1, String host2) {
    return hostToRackMap.get(host1).equals(hostToRackMap.get(host2));
  }

  /**
   * This method computes the host to rack map from Yarn.
   * Only the hosts that are running in the cluster will be a part of this map.
   * @return map of the host and the rack it resides on
   */
  private Multimap<String, FaultDomain> computeHostToFaultDomainMap() {
    YarnClientImpl yarnClient = new YarnClientImpl();
    Multimap<String, FaultDomain> hostToRackMap = HashMultimap.create();
    try {
      List<NodeReport> nodeReport = yarnClient.getNodeReports(NodeState.RUNNING);
      nodeReport.forEach(report -> {
        FaultDomain rack = new FaultDomain(FaultDomainType.RACK, report.getRackName());
        hostToRackMap.put(report.getNodeId().getHost(), rack);
      });
    } catch (YarnException | IOException e) {
      e.printStackTrace();
    }
    return hostToRackMap;
  }
}
