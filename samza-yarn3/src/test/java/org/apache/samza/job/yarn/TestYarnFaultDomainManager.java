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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.clustermanager.FaultDomain;
import org.apache.samza.clustermanager.FaultDomainType;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestYarnFaultDomainManager {
  private final Multimap<String, FaultDomain> hostToRackMap = HashMultimap.create();
  private final String hostName1 = "host1";
  private final String hostName2 = "host2";
  private final String hostName3 = "host3";
  private final String hostName4 = "host4";
  private final String hostName5 = "host5";
  private final String hostName6 = "host6";
  private final String rackName1 = "rack1";
  private final String rackName2 = "rack2";
  private final String rackName3 = "rack3";

  private final NodeReport nodeReport1 = createNodeReport(hostName1, 1, NodeState.RUNNING, "httpAddress1",
          rackName1, 1, 1, 2, 1, 2,
          "", 60L);
  private final NodeReport nodeReport2 = createNodeReport(hostName2, 1, NodeState.RUNNING, "httpAddress2",
          rackName2, 1, 1, 2, 1, 2,
          "", 60L);
  private final NodeReport nodeReport3 = createNodeReport(hostName3, 1, NodeState.RUNNING, "httpAddress3",
          rackName1, 1, 1, 2, 1, 2,
          "", 60L);
  private final NodeReport nodeReport4 = createNodeReport(hostName4, 1, NodeState.RUNNING, "httpAddress4",
          rackName2, 1, 1, 2, 1, 2,
          "", 60L);
  private final NodeReport nodeReport5 = createNodeReport(hostName5, 1, NodeState.RUNNING, "httpAddress5",
          rackName3, 1, 1, 2, 1, 2,
          "", 60L);
  private final NodeReport nodeReport6 = createNodeReport(hostName6, 1, NodeState.RUNNING, "httpAddress6",
          rackName1, 1, 1, 2, 1, 2,
          "", 60L);

  @Mock
  YarnClientImpl yarnClient;
  @Mock
  ReadableMetricsRegistry mockMetricsRegistry;
  @Mock
  Counter mockCounter;

  @Before
  public void setup() {
    FaultDomain rack1 = new FaultDomain(FaultDomainType.RACK, rackName1);
    FaultDomain rack2 = new FaultDomain(FaultDomainType.RACK, rackName2);
    FaultDomain rack3 = new FaultDomain(FaultDomainType.RACK, rackName3);
    hostToRackMap.put(hostName1, rack1);
    hostToRackMap.put(hostName2, rack2);
    hostToRackMap.put(hostName3, rack1);
    hostToRackMap.put(hostName4, rack2);
    hostToRackMap.put(hostName5, rack3);

    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mockCounter);
  }

  @Test
  public void testGetFaultDomainOfHostWhichExistsInCache() {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, hostToRackMap);

    Set<FaultDomain> expectedFaultDomainSet = new HashSet<>();
    expectedFaultDomainSet.add(new FaultDomain(FaultDomainType.RACK, rackName1));

    Set<FaultDomain> actualFaultDomainSet = yarnFaultDomainManager.getFaultDomainsForHost(hostName3);

    assertNotNull(actualFaultDomainSet);
    assertEquals(expectedFaultDomainSet.iterator().next(), actualFaultDomainSet.iterator().next());
    verify(mockCounter, times(0)).inc();
  }

  @Test
  public void testGetFaultDomainOfHostWhichDoesNotExistInCache() throws IOException, YarnException {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, hostToRackMap);

    Set<FaultDomain> expectedFaultDomainSet = new HashSet<>();
    expectedFaultDomainSet.add(new FaultDomain(FaultDomainType.RACK, rackName1));

    List<NodeReport> updatedNodeReport = ImmutableList.of(nodeReport1, nodeReport2, nodeReport3, nodeReport4, nodeReport5, nodeReport6);
    when(yarnClient.getNodeReports(NodeState.RUNNING)).thenReturn(updatedNodeReport);

    Set<FaultDomain> actualFaultDomainSet = yarnFaultDomainManager.getFaultDomainsForHost(hostName6);

    assertNotNull(actualFaultDomainSet);
    assertEquals(expectedFaultDomainSet.iterator().next(), actualFaultDomainSet.iterator().next());
    verify(mockCounter, times(1)).inc();
  }

  @Test
  public void testHasSameFaultDomainsWhenTrue() {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, hostToRackMap);

    boolean result = yarnFaultDomainManager.hasSameFaultDomains(hostName1, hostName3);

    assertTrue(result);
  }

  @Test
  public void testHasSameFaultDomainsWhenFalse() {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, hostToRackMap);

    boolean result = yarnFaultDomainManager.hasSameFaultDomains(hostName1, hostName2);

    assertFalse(result);
  }

  @Test
  public void testHasSameFaultDomainsWhenHostDoesNotExistInCache() throws IOException, YarnException {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, hostToRackMap);

    List<NodeReport> updatedNodeReport = ImmutableList.of(nodeReport1, nodeReport2, nodeReport3, nodeReport4, nodeReport5, nodeReport6);
    when(yarnClient.getNodeReports(NodeState.RUNNING)).thenReturn(updatedNodeReport);

    boolean result = yarnFaultDomainManager.hasSameFaultDomains(hostName1, hostName6);

    assertTrue(result);
  }

  @Test
  public void testComputeHostToFaultDomainMap() throws IOException, YarnException {
    YarnFaultDomainManager yarnFaultDomainManager = new YarnFaultDomainManager(mockMetricsRegistry, yarnClient, null);

    List<NodeReport> nodeReport = ImmutableList.of(nodeReport1, nodeReport2, nodeReport3, nodeReport4, nodeReport5);
    when(yarnClient.getNodeReports(NodeState.RUNNING)).thenReturn(nodeReport);

    Multimap<String, FaultDomain> hostToRackMap = yarnFaultDomainManager.computeHostToFaultDomainMap();

    assertEquals(this.hostToRackMap.size(), hostToRackMap.size());
    assertEquals(this.hostToRackMap.keySet(), hostToRackMap.keySet());
    Iterator<FaultDomain> expectedValues = this.hostToRackMap.values().iterator();
    Iterator<FaultDomain> computedValues = hostToRackMap.values().iterator();
    expectedValues.forEachRemaining(expectedRack -> assertFaultDomainEquals(expectedRack, computedValues.next()));
  }

  private void assertFaultDomainEquals(FaultDomain faultDomain1, FaultDomain faultDomain2) {
    assertEquals(faultDomain1.getType(), faultDomain2.getType());
    assertEquals(faultDomain1.getId(), faultDomain2.getId());
  }

  private NodeReport createNodeReport(String host, int port, NodeState nodeState, String httpAddress, String rackName,
                                      int memoryUsed, int vcoresUsed, int totalMemory, int totalVcores, int numContainers,
                                      String healthReport, long lastHealthReportTime) {
    return NodeReport.newInstance(
            NodeId.newInstance(host, port),
            nodeState,
            httpAddress,
            rackName,
            Resource.newInstance(memoryUsed, vcoresUsed),
            Resource.newInstance(totalMemory, totalVcores),
            numContainers,
            healthReport,
            lastHealthReportTime
    );
  }
}
