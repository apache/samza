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

import org.I0Itec.zkclient.ZkConnection;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestZkProcessorLatch {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(TestZkLeaderElector.class);

  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private static EmbeddedZookeeper zkServer = null;
  private static String zkConnectionString;
  private String testZkConnectionString = null;
  private ZkUtils testZkUtils = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;

  private final CoordinationServiceFactory factory = new ZkCoordinationServiceFactory();

  @BeforeClass
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    zkConnectionString = "localhost:" + zkServer.getPort();
  }
  @Before
  public void testSetup() {
    testZkConnectionString = "127.0.0.1:" + zkServer.getPort();
    try {
      testZkUtils = getZkUtilsWithNewClient("testZkUtils");
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    testZkUtils.connect();
  }

  @After
  public void testTeardown() {
    testZkUtils.deleteRoot();
    testZkUtils.close();
  }

  private Config getConfig() {
    Map<String, String> map = new HashMap<>();
    map.put(ZkConfig.ZK_CONNECT, zkConnectionString);
    return new MapConfig(map);
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testLatchSizeOne() {
    final int latchSize = 1;
    final String latchId = "latchSizeOne";
    final String participant1 = "participant1";

    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future processor = pool.submit(
        () -> {
          ZkUtils zkUtils = getZkUtilsWithNewClient(participant1);
          zkUtils.connect();
          ZkProcessorLatch latch = new ZkProcessorLatch(
              latchSize, latchId, participant1, new ZkConfig(getConfig()), zkUtils);
          latch.countDown();
          try {
            latch.await(30, TimeUnit.SECONDS);
          } catch (Exception e) {
            Assert.fail("Threw an exception while waiting for latch completion in participant1!" + e.getLocalizedMessage());
          } finally {
            zkUtils.close();
          }
      });

    try {
       processor.get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      Assert.fail("failed to get future." + e.getLocalizedMessage());
    } finally {
      pool.shutdownNow();
    }
    try {
      List<String> latchParticipants =
          testZkUtils.getZkClient().getChildren(
              String.format("%s/%s_%s", KEY_BUILDER.getRootPath(), ZkProcessorLatch.LATCH_PATH, latchId));
      Assert.assertNotNull(latchParticipants);
      Assert.assertEquals(1, latchParticipants.size());
      Assert.assertEquals("0000000000", latchParticipants.get(0));
    } catch (Exception e) {
      Assert.fail("Failed to read the latch status from ZK directly" + e.getLocalizedMessage());
    }
  }

//  @Test
  public void testLatchSizeOneWithTwoParticipants() {
    int latchSize = 1;
    String latchId = "testLatchSizeOneWithTwoParticipants";
    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future f1 = pool.submit(
        () -> {
          CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant1", getConfig());
          Latch latch = coordinationUtils.getLatch(latchSize, latchId);
          //latch.countDown(); only one thread counts down
          try {
            latch.await(100000, TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            Assert.fail("await timed out. " + e.getLocalizedMessage());
          }
        });

    Future f2 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant2", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });

    try {
      f1.get(30000, TimeUnit.MILLISECONDS);
      f2.get(30000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("failed to get future." + e.getLocalizedMessage());
    }
    pool.shutdownNow();
  }

//  @Test
  public void testLatchSizeN() {
    int latchSize = 3;
    String latchId = "testLatchSizeN";

    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future f1 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils;
        Latch latch = null;
        try {
          coordinationUtils = factory.getCoordinationService("groupId", "participant1", getConfig());
          latch = coordinationUtils.getLatch(latchSize, latchId);
          latch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
        }
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          System.out.println("######## Future 1 #############");
          e.printStackTrace();
          Assert.fail("await timed out " + e.getLocalizedMessage());
        }
      });
    Future f2 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant2", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          System.out.println("######## Future 2 #############");
          e.printStackTrace();
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });
    Future f3 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant3", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          System.out.println("######## Future 3 #############");
          e.printStackTrace();
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });

    try {
      f1.get(300, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("failed to get future1 " + e.getLocalizedMessage());
    }
    try {
      f2.get(300, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("failed to get future2 " + e.getLocalizedMessage());
    }
    try {
      f3.get(300, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("failed to get future3 " + e.getLocalizedMessage());
    }
  }

//  @Test
  public void testLatchExpires() {
    String latchId = "l4";
    int latchSize = 3;

    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future f1 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant1", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });
    Future f2 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant2", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });
    Future f3 = pool.submit(
      () -> {
        CoordinationUtils coordinationUtils = factory.getCoordinationService("groupId", "participant3", getConfig());
        Latch latch = coordinationUtils.getLatch(latchSize, latchId);
        // This processor never completes its task
        //latch.countDown();
        try {
          latch.await(100000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          Assert.fail("await timed out. " + e.getLocalizedMessage());
        }
      });

    try {
      f1.get(300, TimeUnit.MILLISECONDS);
      f2.get(300, TimeUnit.MILLISECONDS);
      f3.get(300, TimeUnit.MILLISECONDS);
      Assert.fail("Latch should've timeout.");
    } catch (Exception e) {
      f1.cancel(true);
      f2.cancel(true);
      f3.cancel(true);
      // expected
    }
    pool.shutdownNow();
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
