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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationService;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

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
  private final CoordinationServiceFactory factory = new ZkCoordinationServiceFactory();

  @BeforeClass
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @Before
  public void testSetup() {
    testZkConnectionString = "localhost:" + zkServer.getPort();
    try {
      testZkUtils = getZkUtilsWithNewClient("testProcessorId");
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    try {
      testZkUtils.getZkClient().createPersistent(KEY_BUILDER.getProcessorsPath(), true);
    } catch (ZkNodeExistsException e) {
      // Do nothing
    }
  }

  public static class BooleanResult {
    public boolean res = false;
  }

  @After
  public void testTeardown() {
    testZkUtils.deleteRoot();
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
        add("0000000000");
      }
    };

    ZkUtils mockZkUtils = mock(ZkUtils.class);
    when(mockZkUtils.registerProcessorAndGetId(any())).
        thenReturn(KEY_BUILDER.getProcessorsPath() + "/0000000000");
    when(mockZkUtils.getSortedActiveProcessors()).thenReturn(activeProcessors);
    Mockito.doNothing().when(mockZkUtils).makeSurePersistentPathsExists(any(String[].class));

    ZkKeyBuilder kb = mock(ZkKeyBuilder.class);
    when(kb.getProcessorsPath()).thenReturn("");
    when(mockZkUtils.getKeyBuilder()).thenReturn(kb);

    ZkLeaderElector leaderElector = new ZkLeaderElector("1", mockZkUtils, null);
    BooleanResult isLeader = new BooleanResult();

    leaderElector.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader.res = true;
      }
    });
    Assert.assertTrue(TestZkUtils.testWithDelayBackOff(() -> isLeader.res, 2, 100));
  }

  @Test
  public void testUnregisteredProcessorInLeaderElection() {
    String processorId = "1";
    ZkUtils mockZkUtils = mock(ZkUtils.class);
    when(mockZkUtils.getSortedActiveProcessors()).thenReturn(new ArrayList<String>());
    Mockito.doNothing().when(mockZkUtils).makeSurePersistentPathsExists(any(String[].class));

    ZkKeyBuilder kb = mock(ZkKeyBuilder.class);
    when(kb.getProcessorsPath()).thenReturn("");
    when(mockZkUtils.getKeyBuilder()).thenReturn(kb);

    ZkLeaderElector leaderElector = new ZkLeaderElector(processorId, mockZkUtils, null);

    try {
      leaderElector.tryBecomeLeader(new LeaderElectorListener() {
        @Override
        public void onBecomingLeader() {
        }
      });
      Assert.fail("Was expecting leader election to fail!");
    } catch (SamzaException e) {
      // No-op Expected
    }
  }

  private CoordinationService getZkCoordinationService(String groupId, String processorId) {

    Map<String, String> map = new HashMap<>();
    map.put(ZkConfig.ZK_CONNECT, testZkConnectionString);
    Config config = new MapConfig(map);

    CoordinationService coordinationService = factory.getCoordinationService(groupId, processorId, config);
    coordinationService.start();

    return coordinationService;
  }

  /**
   * Test starts 3 processors and verifies the state of the Zk tree after all processors participate in LeaderElection
   */
  @Test
  public void testLeaderElection() {
    BooleanResult isLeader1 = new BooleanResult();
    BooleanResult isLeader2 = new BooleanResult();
    BooleanResult isLeader3 = new BooleanResult();


    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient("1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector("1", zkUtils1, null);

    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient("2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector("2", zkUtils2, null);


    // Processor-3
    ZkUtils zkUtils3 = getZkUtilsWithNewClient("3");
    ZkLeaderElector leaderElector3 = new ZkLeaderElector("3", zkUtils3, null);

    Assert.assertEquals(0, testZkUtils.getSortedActiveProcessors().size());

    leaderElector1.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader1.res = true;
      }
    });
    leaderElector2.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader2.res = true;
      }
    });
    leaderElector3.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader3.res = true;
      }
    });

    Assert.assertTrue(TestZkUtils.testWithDelayBackOff(() -> isLeader1.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader2.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader3.res, 2, 100));

    Assert.assertEquals(3, testZkUtils.getSortedActiveProcessors().size());


    // Clean up
    zkUtils1.close();
    zkUtils2.close();
    zkUtils3.close();

    Assert.assertEquals(new ArrayList<String>(), testZkUtils.getSortedActiveProcessors());

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

    BooleanResult isLeader1 = new BooleanResult();
    BooleanResult isLeader2 = new BooleanResult();
    BooleanResult isLeader3 = new BooleanResult();


    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient("processor1");
    zkUtils1.registerProcessorAndGetId("processor1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector("processor1",zkUtils1, null);

    leaderElector1.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        count.incrementAndGet();
      }
    });

    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient("processor2");
    final String path2 = zkUtils2.registerProcessorAndGetId("processor2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector("processor2", zkUtils2, null);

    leaderElector2.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        String registeredIdStr = ZkKeyBuilder.parseIdFromPath(path2);
        Assert.assertNotNull(registeredIdStr);

        String predecessorIdStr = ZkKeyBuilder.parseIdFromPath(dataPath);
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
    ZkUtils zkUtils3  = getZkUtilsWithNewClient("processor3");
    zkUtils3.registerProcessorAndGetId("processor3");
    ZkLeaderElector leaderElector3 = new ZkLeaderElector("processor3", zkUtils3, null);

    leaderElector3.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        count.incrementAndGet();
      }
    });

    // Join Leader Election
    leaderElector1.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader1.res = true;
      }
    });
    leaderElector2.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader2.res = true;
      }
    });
    leaderElector3.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader3.res = true;
      }
    });

    Assert.assertTrue(TestZkUtils.testWithDelayBackOff(() -> isLeader1.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader2.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader3.res, 2, 100));

    Assert.assertTrue(leaderElector1.amILeader());
    Assert.assertFalse(leaderElector2.amILeader());
    Assert.assertFalse(leaderElector3.amILeader());

    List<String> currentActiveProcessors = zkUtils1.getSortedActiveProcessors();
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
    Assert.assertEquals(currentActiveProcessors, zkUtils2.getSortedActiveProcessors());

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

    BooleanResult isLeader1 = new BooleanResult();
    BooleanResult isLeader2 = new BooleanResult();
    BooleanResult isLeader3 = new BooleanResult();

    // Processor-1
    ZkUtils zkUtils1 = getZkUtilsWithNewClient("processor1");
    zkUtils1.registerProcessorAndGetId("processor1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector("processor1", zkUtils1, null);

    leaderElector1.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {

      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        count.incrementAndGet();
      }
    });


    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient("processor2");
    zkUtils2.registerProcessorAndGetId("processor2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector("processor2", zkUtils2, null);

    leaderElector2.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {

      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        count.incrementAndGet();
      }
    });

    // Processor-3
    ZkUtils zkUtils3  = getZkUtilsWithNewClient("processor3");
    final String path3 = zkUtils3.registerProcessorAndGetId("processor3");
    ZkLeaderElector leaderElector3 = new ZkLeaderElector("processor3", zkUtils3, null);

    leaderElector3.setPreviousProcessorChangeListener(new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {

      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        String registeredIdStr = ZkKeyBuilder.parseIdFromPath(path3);
        Assert.assertNotNull(registeredIdStr);

        String predecessorIdStr = ZkKeyBuilder.parseIdFromPath(dataPath);
        Assert.assertNotNull(predecessorIdStr);

        try {
          int selfId = Integer.parseInt(registeredIdStr);
          int predecessorId = Integer.parseInt(predecessorIdStr);
          Assert.assertEquals(1, selfId - predecessorId);
        } catch (Exception e) {
          Assert.fail("Exception in LeaderElectionListener!");
        }
        count.incrementAndGet();
        electionLatch.countDown();
      }
    });

    // Join Leader Election
    leaderElector1.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader1.res = true;
      }
    });
    leaderElector2.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader2.res = true;
      }
    });
    leaderElector3.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader3.res = true;
      }
    });
    Assert.assertTrue(TestZkUtils.testWithDelayBackOff(() -> isLeader1.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader2.res, 2, 100));
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> isLeader3.res, 2, 100));

    List<String> currentActiveProcessors = zkUtils1.getSortedActiveProcessors();
    Assert.assertEquals(3, currentActiveProcessors.size());

    zkUtils2.close();
    currentActiveProcessors.remove(1);

    try {
      Assert.assertTrue(electionLatch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Assert.fail("Interrupted while waiting for leaderElection listener callback to complete!");
    }

    Assert.assertEquals(1, count.get());
    Assert.assertEquals(currentActiveProcessors, zkUtils1.getSortedActiveProcessors());

    // Clean up
    zkUtils1.close();
    zkUtils3.close();
  }

  @Test
  public void testAmILeader() {
    BooleanResult isLeader1 = new BooleanResult();
    BooleanResult isLeader2 = new BooleanResult();
    // Processor-1

    ZkUtils zkUtils1 = getZkUtilsWithNewClient("1");
    ZkLeaderElector leaderElector1 = new ZkLeaderElector("1", zkUtils1, null);

    // Processor-2
    ZkUtils zkUtils2 = getZkUtilsWithNewClient("2");
    ZkLeaderElector leaderElector2 = new ZkLeaderElector("2", zkUtils2, null);

    // Before Leader Election
    Assert.assertFalse(leaderElector1.amILeader());
    Assert.assertFalse(leaderElector2.amILeader());

    leaderElector1.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader1.res = true;
      }
    });
    leaderElector2.tryBecomeLeader(new LeaderElectorListener() {
      @Override
      public void onBecomingLeader() {
        isLeader2.res = true;
      }
    });

    // After Leader Election
    Assert.assertTrue(leaderElector1.amILeader());
    Assert.assertFalse(leaderElector2.amILeader());

    zkUtils1.close();
    zkUtils2.close();
  }

  private ZkUtils getZkUtilsWithNewClient(String processorId) {
    ZkConnection zkConnection = ZkUtils.createZkConnection(testZkConnectionString, SESSION_TIMEOUT_MS);
    return new ZkUtils(
        processorId,
        KEY_BUILDER,
        ZkUtils.createZkClient(zkConnection, CONNECTION_TIMEOUT_MS),
        CONNECTION_TIMEOUT_MS);
  }
}
