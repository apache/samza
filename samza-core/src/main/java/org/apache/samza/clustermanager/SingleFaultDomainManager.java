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

import java.util.Collections;
import java.util.Set;


/**
 * Simple placeholder implementation of {@link FaultDomainManager} which contains a single fault domain for all hosts.
 * This can be used when another concrete {@link FaultDomainManager} is undesirable or unavailable, but features which
 * depend on a {@link FaultDomainManager} (such as standby containers) may have unexpected behavior.
 */
public class SingleFaultDomainManager implements FaultDomainManager {
  private static final FaultDomain SINGLE_FAULT_DOMAIN = new FaultDomain(FaultDomainType.RACK, "0");

  @Override
  public Set<FaultDomain> getAllFaultDomains() {
    return Collections.singleton(SINGLE_FAULT_DOMAIN);
  }

  @Override
  public Set<FaultDomain> getFaultDomainsForHost(String host) {
    return Collections.singleton(SINGLE_FAULT_DOMAIN);
  }

  @Override
  public boolean hasSameFaultDomains(String host1, String host2) {
    return true;
  }
}
