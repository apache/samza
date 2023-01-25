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
package org.apache.samza.zk;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.junit.Test;

import static junit.framework.Assert.*;

public class TestZkClusterMembership {
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private ZkUtils zkUtils1;
  private ZkUtils zkUtils2;

  @BeforeClass
  public static void test() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = String.format("127.0.0.1:%d", zkServer.getPort());
  }

  @Before
  public void testSetup() {
    ZkClient zkClient1 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils1 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient1, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
    ZkClient zkClient2 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils2 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient2, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
  }

  @After
  public void testTearDown() {
    zkUtils1.close();
    zkUtils2.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testMembershipSingleProcessor() {
    // happy path for single processor
    ZkClusterMembership clusterMembership = new ZkClusterMembership("p1", zkUtils1);
    String processorId;

    assertEquals("ClusterMembership has participants before any processor registered.", 0, clusterMembership.getNumberOfProcessors());

    processorId = clusterMembership.registerProcessor();

    assertEquals("ClusterMembership does not have participants after a processor registered.", 1, clusterMembership.getNumberOfProcessors());

    clusterMembership.unregisterProcessor(processorId);

    assertEquals("ClusterMembership has participants after the single processor unregistered.", 0, clusterMembership.getNumberOfProcessors());
  }

  @Test
  public void testMembershipTwoProcessors() {
    // Two processors register. Check if second processor registering gets 2 as number of processors.
    ZkClusterMembership clusterMembership1 = new ZkClusterMembership("p1", zkUtils1);
    ZkClusterMembership clusterMembership2 = new ZkClusterMembership("p2", zkUtils1);

    String processorId1;
    String processorId2;

    assertEquals("ClusterMembership has participants before any processor registered.", 0, clusterMembership1.getNumberOfProcessors());

    processorId1 = clusterMembership1.registerProcessor();

    assertEquals("ClusterMembership does not have participants after one processor registered.", 1, clusterMembership1.getNumberOfProcessors());

    processorId2 = clusterMembership2.registerProcessor();

    assertEquals("ClusterMembership does not have 2 participants after two processor registered.", 2, clusterMembership2.getNumberOfProcessors());

    clusterMembership1.unregisterProcessor(processorId1);
    clusterMembership2.unregisterProcessor(processorId2);

    assertEquals("ClusterMembership has participants after both processors unregistered.", 0, clusterMembership1.getNumberOfProcessors());
  }

  @Test
  public void testMembershipFirstProcessorUnregister() {
    // First processor unregisters. Check if second processor registering gets 1 as number of processors.
    ZkClusterMembership clusterMembership1 = new ZkClusterMembership("p1", zkUtils1);
    ZkClusterMembership clusterMembership2 = new ZkClusterMembership("p2", zkUtils1);

    String processorId1;
    String processorId2;

    assertEquals("ClusterMembership has participants before any processor registered.", 0, clusterMembership1.getNumberOfProcessors());

    processorId1 = clusterMembership1.registerProcessor();

    assertEquals("ClusterMembership does not have participants after one processor registered.", 1, clusterMembership1.getNumberOfProcessors());

    clusterMembership1.unregisterProcessor(processorId1);

    processorId2 = clusterMembership2.registerProcessor();

    assertEquals("ClusterMembership does not have 1 participant1 after second processor registered.", 1, clusterMembership2.getNumberOfProcessors());

    clusterMembership2.unregisterProcessor(processorId2);

    assertEquals("ClusterMembership has participants after both processors unregistered.", 0, clusterMembership2.getNumberOfProcessors());
  }
}
