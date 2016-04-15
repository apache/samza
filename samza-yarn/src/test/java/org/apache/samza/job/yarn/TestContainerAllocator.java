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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.yarn.util.MockContainerRequestState;
import org.apache.samza.job.yarn.util.TestUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainerAllocator extends TestContainerAllocatorCommon {

  private final Config config = new MapConfig(new HashMap<String, String>() {
    {
      put("yarn.container.count", "1");
      put("systems.test-system.samza.factory", "org.apache.samza.job.yarn.MockSystemFactory");
      put("yarn.container.memory.mb", "512");
      put("yarn.package.path", "/foo");
      put("task.inputs", "test-system.test-stream");
      put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
      put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
      put("yarn.container.retry.count", "1");
      put("yarn.container.retry.window.ms", "1999999999");
      put("yarn.container.request.timeout.ms", "3");
      put("yarn.allocator.sleep.ms", "1");
    }
  });

  @Override
  protected Config getConfig() {
    return config;
  }

  @Override
  protected MockContainerRequestState createContainerRequestState(
      AMRMClientAsync<AMRMClient.ContainerRequest> amClient) {
    return new MockContainerRequestState(amClient, false);
  }

  /**
   * Test request containers with no containerToHostMapping makes the right number of requests
   */
  @Test
  public void testRequestContainersWithNoMapping() throws Exception {
    int containerCount = 4;
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>();
    for (int i = 0; i < containerCount; i++) {
      containersToHostMapping.put(i, null);
    }
    allocatorThread.start();

    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(requestState);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertTrue(requestState.getRequestsToCountMap().keySet().size() == 0);
  }
  /**
   * Adds all containers returned to ANY_HOST only
   */
  @Test
  public void testAddContainer() throws Exception {
    assertNull(requestState.getContainersOnAHost("host1"));
    assertNull(requestState.getContainersOnAHost(ANY_HOST));

    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "host1", 123));
    containerAllocator.addContainer(TestUtil.getContainer(ConverterUtils.toContainerId("container_1350670447861_0003_01_000003"), "host3", 123));

    assertNull(requestState.getContainersOnAHost("host1"));
    assertNotNull(requestState.getContainersOnAHost(ANY_HOST));
    assertTrue(requestState.getContainersOnAHost(ANY_HOST).size() == 2);
  }

  /**
   * Test requestContainers
   */
  @Test
  public void testRequestContainers() throws Exception {
    Map<Integer, String> containersToHostMapping = new HashMap<Integer, String>() {
      {
        put(0, "host1");
        put(1, "host2");
        put(2, null);
        put(3, "host1");
      }
    };

    allocatorThread.start();

    containerAllocator.requestContainers(containersToHostMapping);

    assertNotNull(testAMRMClient.requests);
    assertEquals(4, testAMRMClient.requests.size());

    assertNotNull(requestState);

    assertNotNull(requestState.getRequestsQueue());
    assertTrue(requestState.getRequestsQueue().size() == 4);

    // If host-affinty is not enabled, it doesn't update the requestMap
    assertNotNull(requestState.getRequestsToCountMap());
    assertTrue(requestState.getRequestsToCountMap().keySet().size() == 0);
  }

}
