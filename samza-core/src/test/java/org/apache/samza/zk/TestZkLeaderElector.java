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

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.SamzaException;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestZkLeaderElector {

  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private String testZkConnectionString = null;
  private ZkUtils testZkUtils = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;

  @BeforeClass
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @Before
  public void testSetup() {
    testZkConnectionString = "localhost:" + zkServer.getPort();
    try {
      testZkUtils = getZkUtilsWithNewClient();
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    try {
      testZkUtils.getZkClient().createPersistent(KEY_BUILDER.getProcessorsPath(), true);
    } catch (ZkNodeExistsException e) {
      // Do nothing
    }
  }


  @After
  public void testTeardown() {
    testZkUtils.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testLeaderElectionRegistersProcessor() {
    List<String> activeProcessors = new ArrayList<String>() {
      {
        add("processor-0000000000");
      }
    };

    ZkUtils mockZkUtils = mock(ZkUtils.class);

    when(mockZkUtils.getEphemeralPath()).thenReturn(null);
    when(mockZkUtils.registerProcessorAndGetId(any())).
        thenReturn(KEY_BUILDER.getProcessorsPath() + "/processor-0000000000");
    when(mockZkUtils.getActiveProcessors()).thenReturn(activeProcessors);

    ZkLeaderElector leaderElector = new ZkLeaderElector("1", mockZkUtils);
    Assert.assertTrue(leaderElector.tryBecomeLeader());
  }

  @Test
  public void testUnregisteredProcessorInLeaderElection() {
    String processorId = "1";
    ZkUtils mockZkUtils = mock(ZkUtils.class);
    when(mockZkUtils.getEphemeralPath()).thenReturn("/test/processors/processor-dummy");
    when(mockZkUtils.getActiveProcessors()).thenReturn(new ArrayList<String>());

    ZkLeaderElector leaderElector = new ZkLeaderElector(processorId, mockZkUtils);
    try {
      leaderElector.tryBecomeLeader();
      Assert.fail("Was expecting leader election to fail!");
    } catch (SamzaException e) {
      // No-op Expected
    }
  }

  /**
   * Test starts 3 processors and verifies the state of the Zk tree after all processors participate in LeaderElection
   */
  @Test
  public void testLeaderElection() {
    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient();
    ZkLeaderElector leaderElector1 = new ZkLeaderElector(
        "1",
        zkUtils1);

    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient();
    ZkLeaderElector leaderElector2 = new ZkLeaderElector(
        "2",
        zkUtils2);

    // Processor-3
    ZkUtils zkUtils3  = getZkUtilsWithNewClient();
    ZkLeaderElector leaderElector3 = new ZkLeaderElector(
        "3",
        zkUtils3);

    Assert.assertEquals(0, testZkUtils.getActiveProcessors().size());

    Assert.assertTrue(leaderElector1.tryBecomeLeader());
    Assert.assertFalse(leaderElector2.tryBecomeLeader());
    Assert.assertFalse(leaderElector3.tryBecomeLeader());

    Assert.assertEquals(3, testZkUtils.getActiveProcessors().size());

    // Clean up
    zkUtils1.close();
    zkUtils2.close();
    zkUtils3.close();

    Assert.assertEquals(new ArrayList<String>(), testZkUtils.getActiveProcessors());

  }

  /**
   * Tests that Leader Failure automatically promotes the next successor to become the leader
   */
  @Test
  public void testLeaderFailure() {
    /**
     * electionLatch and count together verify that:
     * 1. the registered listeners are actually invoked by the ZkClient on the correct path
     * 2. for a single participant failure, at-most 1 other participant is notified
     */
    final CountDownLatch electionLatch = new CountDownLatch(1);
    final AtomicInteger count = new AtomicInteger(0);

    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient();
    zkUtils1.registerProcessorAndGetId("processor1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector(
        "1",
        zkUtils1,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            count.incrementAndGet();
          }
        });


    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient();
    final String path2 = zkUtils2.registerProcessorAndGetId("processor2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector(
        "2",
        zkUtils2,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            String registeredIdStr = path2.substring(path2.indexOf("-") + 1);
            Assert.assertNotNull(registeredIdStr);

            String predecessorIdStr = dataPath.substring(dataPath.indexOf("-") + 1);
            Assert.assertNotNull(predecessorIdStr);

            try {
              int selfId = Integer.parseInt(registeredIdStr);
              int predecessorId = Integer.parseInt(predecessorIdStr);
              Assert.assertEquals(1, selfId - predecessorId);
            } catch (Exception e) {
              System.out.println(e.getMessage());
            }
            count.incrementAndGet();
            electionLatch.countDown();
          }
        });

    // Processor-3
    ZkUtils zkUtils3  = getZkUtilsWithNewClient();
    zkUtils3.registerProcessorAndGetId("processor3");
    ZkLeaderElector leaderElector3 = new ZkLeaderElector(
        "3",
        zkUtils3,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            count.incrementAndGet();
          }
        });

    // Join Leader Election
    Assert.assertTrue(leaderElector1.tryBecomeLeader());
    Assert.assertFalse(leaderElector2.tryBecomeLeader());
    Assert.assertFalse(leaderElector3.tryBecomeLeader());

    List<String> currentActiveProcessors = testZkUtils.getActiveProcessors();
    System.out.println(currentActiveProcessors);
    Assert.assertEquals(3, currentActiveProcessors.size());

    // Leader Failure
    zkUtils1.close();
    currentActiveProcessors.remove(0);

    try {
      Assert.assertTrue(electionLatch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Assert.fail("Interrupted while waiting for leaderElection listener callback to complete!");
    }

    Assert.assertEquals(1, count.get());
    Assert.assertEquals(currentActiveProcessors, testZkUtils.getActiveProcessors());

    // Clean up
    zkUtils2.close();
    zkUtils3.close();
  }

  /**
   * Tests that a non-leader failure updates the Zk tree and participants' state correctly
   */
  @Test
  public void testNonLeaderFailure() {
    /**
     * electionLatch and count together verify that:
     * 1. the registered listeners are actually invoked by the ZkClient on the correct path
     * 2. for a single participant failure, at-most 1 other participant is notified
     */
    final CountDownLatch electionLatch = new CountDownLatch(1);
    final AtomicInteger count = new AtomicInteger(0);

    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient();
    zkUtils1.registerProcessorAndGetId("processor1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector(
        "1",
        zkUtils1,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            count.incrementAndGet();
          }
        });

    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient();
    zkUtils2.registerProcessorAndGetId("processor2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector(
        "2",
        zkUtils2,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            count.incrementAndGet();
          }
        });

    // Processor-3
    ZkUtils zkUtils3  = getZkUtilsWithNewClient();
    final String path3 = zkUtils3.registerProcessorAndGetId("processor3");
    ZkLeaderElector leaderElector3 = new ZkLeaderElector(
        "3",
        zkUtils3,
        new IZkDataListener() {
          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception {

          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception {
            String registeredIdStr = path3.substring(path3.indexOf("-") + 1);
            Assert.assertNotNull(registeredIdStr);

            String predecessorIdStr = dataPath.substring(dataPath.indexOf("-") + 1);
            Assert.assertNotNull(predecessorIdStr);

            try {
              int selfId = Integer.parseInt(registeredIdStr);
              int predecessorId = Integer.parseInt(predecessorIdStr);
              Assert.assertEquals(1, selfId - predecessorId);
            } catch (Exception e) {
              System.out.println(e.getMessage());
            }
            count.incrementAndGet();
            electionLatch.countDown();
          }
        });

    // Join Leader Election
    Assert.assertTrue(leaderElector1.tryBecomeLeader());
    Assert.assertFalse(leaderElector2.tryBecomeLeader());
    Assert.assertFalse(leaderElector3.tryBecomeLeader());

    List<String> currentActiveProcessors = testZkUtils.getActiveProcessors();
    Assert.assertEquals(3, currentActiveProcessors.size());

    zkUtils2.close();
    currentActiveProcessors.remove(1);

    try {
      Assert.assertTrue(electionLatch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Assert.fail("Interrupted while waiting for leaderElection listener callback to complete!");
    }

    Assert.assertEquals(1, count.get());
    Assert.assertEquals(currentActiveProcessors, testZkUtils.getActiveProcessors());

    // Clean up
    zkUtils1.close();
    zkUtils3.close();
  }

  @Test
  public void testAmILeader() {
    String zkConnectionString = "localhost:" + zkServer.getPort();
    // Processor-1
    ZkLeaderElector leaderElector1 = new ZkLeaderElector(
        "1",
        getZkUtilsWithNewClient());

    // Processor-2
    ZkLeaderElector leaderElector2 = new ZkLeaderElector(
        "2",
        getZkUtilsWithNewClient());

    // Before Leader Election
    Assert.assertFalse(leaderElector1.amILeader());
    Assert.assertFalse(leaderElector2.amILeader());

    leaderElector1.tryBecomeLeader();
    leaderElector2.tryBecomeLeader();

    // After Leader Election
    Assert.assertTrue(leaderElector1.amILeader());
    Assert.assertFalse(leaderElector2.amILeader());
  }

  private ZkUtils getZkUtilsWithNewClient() {
    ZkConnection zkConnection = ZkUtils.createZkConnection(testZkConnectionString, SESSION_TIMEOUT_MS);
    return new ZkUtils(
        KEY_BUILDER,
        zkConnection,
        ZkUtils.createZkClient(zkConnection, CONNECTION_TIMEOUT_MS),
        CONNECTION_TIMEOUT_MS);
  }
}
