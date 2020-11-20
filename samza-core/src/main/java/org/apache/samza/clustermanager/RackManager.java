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

import java.util.Collection;
import java.util.Map;

public class RackManager {

  private final Map<String, String> nodeToRackMap;

  public RackManager(Map<String, String> nodeToRackMap) {
    this.nodeToRackMap = nodeToRackMap;
  }

  public String getRackOfNode(String host) {
    return nodeToRackMap.get(host);
  }

  public boolean checkHostsOnSameRack(String host1, String host2) {
    return nodeToRackMap.get(host1).equals(nodeToRackMap.get(host2));
  }

  public String[] getAllowedRacksForStandbyContainer(String host) {
    String activeContainerRack = nodeToRackMap.get(host);
    Collection<String> standbyRacks = nodeToRackMap.values();
    standbyRacks.remove(activeContainerRack);
    return standbyRacks.toArray(new String[0]);
  }

}
