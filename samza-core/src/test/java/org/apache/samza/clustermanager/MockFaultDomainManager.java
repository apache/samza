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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.Set;
import org.apache.samza.metrics.MetricsRegistry;

public class MockFaultDomainManager implements FaultDomainManager {

  private final Multimap<String, FaultDomain> hostToFaultDomainMap;

  public MockFaultDomainManager(MetricsRegistry metricsRegistry) {
    FaultDomain faultDomain1 = new FaultDomain(FaultDomainType.RACK, "rack-1");
    FaultDomain faultDomain2 = new FaultDomain(FaultDomainType.RACK, "rack-2");
    FaultDomain faultDomain3 = new FaultDomain(FaultDomainType.RACK, "rack-1");
    FaultDomain faultDomain4 = new FaultDomain(FaultDomainType.RACK, "rack-2");
    FaultDomain faultDomain5 = new FaultDomain(FaultDomainType.RACK, "rack-3");
    hostToFaultDomainMap = HashMultimap.create();
    hostToFaultDomainMap.put("host-1", faultDomain1);
    hostToFaultDomainMap.put("host-2", faultDomain2);
    hostToFaultDomainMap.put("host-3", faultDomain3);
    hostToFaultDomainMap.put("host-4", faultDomain4);
    hostToFaultDomainMap.put("host-5", faultDomain5);
  }

  @Override
  public Set<FaultDomain> getAllFaultDomains() {
    return new HashSet<>(hostToFaultDomainMap.values());
  }

  @Override
  public Set<FaultDomain> getFaultDomainsForHost(String host) {
    return new HashSet<>(hostToFaultDomainMap.get(host));
  }

  @Override
  public boolean hasSameFaultDomains(String host1, String host2) {
    return hostToFaultDomainMap.get(host1).equals(hostToFaultDomainMap.get(host2));
  }

}
