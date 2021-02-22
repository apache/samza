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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.FaultDomain;
import org.apache.samza.clustermanager.FaultDomainManager;
import org.apache.samza.clustermanager.FaultDomainType;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class functionality works with the assumption that the job.standbytasks.replication.factor is 2.
 * For values greater than 2, it is possible that the standby containers could be on the same rack as the active, or the already existing standby racks.
 */
public class YarnFaultDomainManager implements FaultDomainManager {

  private static final Logger log = LoggerFactory.getLogger(FaultDomainManager.class);
  private static final String FAULT_DOMAIN_MANAGER_GROUP = "yarn-fault-domain-manager";
  private static final String HOST_TO_FAULT_DOMAIN_CACHE_UPDATES = "host-to-fault-domain-cache-updates";
  private Multimap<String, FaultDomain> hostToRackMap;
  private final YarnClientImpl yarnClient;
  private Counter hostToFaultDomainCacheUpdates;

  public YarnFaultDomainManager(MetricsRegistry metricsRegistry) {
    this.yarnClient = new YarnClientImpl();
    yarnClient.init(new YarnConfiguration());
    yarnClient.start();
    this.hostToRackMap = computeHostToFaultDomainMap();
    hostToFaultDomainCacheUpdates = metricsRegistry.newCounter(FAULT_DOMAIN_MANAGER_GROUP, HOST_TO_FAULT_DOMAIN_CACHE_UPDATES);
  }

  @VisibleForTesting
  YarnFaultDomainManager(MetricsRegistry metricsRegistry, YarnClientImpl yarnClient, Multimap<String, FaultDomain> hostToRackMap) {
    this.yarnClient = yarnClient;
    yarnClient.init(new YarnConfiguration());
    yarnClient.start();
    this.hostToRackMap = hostToRackMap;
    hostToFaultDomainCacheUpdates = metricsRegistry.newCounter(FAULT_DOMAIN_MANAGER_GROUP, HOST_TO_FAULT_DOMAIN_CACHE_UPDATES);
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
   * This method returns the rack a particular host resides on based on the internal cache.
   * In case the rack of a host does not exist in this cache, we update the cache by computing the host to rack map again using Yarn.
   * @param host the host
   * @return the {@link FaultDomain}
   */
  @Override
  public Set<FaultDomain> getFaultDomainsForHost(String host) {
    if (!hostToRackMap.containsKey(host)) {
      hostToRackMap = computeHostToFaultDomainMap();
      hostToFaultDomainCacheUpdates.inc();
    }
    return new HashSet<>(hostToRackMap.get(host));
  }

  /**
   * This method checks if the two hostnames provided reside on the same rack.
   * @param host1 hostname
   * @param host2 hostname
   * @return true if the hosts exist on the same rack
   */
  @Override
  public boolean hasSameFaultDomains(String host1, String host2) {
    if (!hostToRackMap.keySet().contains(host1) || !hostToRackMap.keySet().contains(host2)) {
      hostToRackMap = computeHostToFaultDomainMap();
      hostToFaultDomainCacheUpdates.inc();
    }
    return hostToRackMap.get(host1).equals(hostToRackMap.get(host2));
  }

  /**
   * This method computes the host to rack map from Yarn.
   * Only the hosts that are running in the cluster will be a part of this map.
   * @return map of the host and the rack it resides on
   */
  @VisibleForTesting
  Multimap<String, FaultDomain> computeHostToFaultDomainMap() {
    Multimap<String, FaultDomain> hostToRackMap = HashMultimap.create();
    try {
      List<NodeReport> nodeReport = yarnClient.getNodeReports(NodeState.RUNNING);
      nodeReport.forEach(report -> {
        FaultDomain rack = new FaultDomain(FaultDomainType.RACK, report.getRackName());
        hostToRackMap.put(report.getNodeId().getHost(), rack);
      });
      log.info("Computed the host to rack map successfully from Yarn.");
    } catch (YarnException | IOException e) {
      throw new SamzaException("Yarn threw an exception while getting NodeReports.", e);
    }
    return hostToRackMap;
  }
}
