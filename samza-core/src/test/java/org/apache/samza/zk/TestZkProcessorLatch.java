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
import org.apache.samza.coordinator.Latch;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestZkProcessorLatch {
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private static EmbeddedZookeeper zkServer = null;
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
              latchSize, latchId, participant1, zkUtils);
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

  @Test
  public void testLatchSizeOneWithTwoParticipants() {
    final int latchSize = 1;
    final String latchId = "testLatchSizeOneWithTwoParticipants";
    final String participant1 = "participant1";
    final String participant2 = "participant2";

    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future f1 = pool.submit(
        () -> {
          ZkUtils zkUtils = getZkUtilsWithNewClient(participant1);
          zkUtils.connect();
          Latch latch = new ZkProcessorLatch(latchSize, latchId, participant1, zkUtils);
          //latch.countDown(); only one thread counts down
          try {
            latch.await(30, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            Assert.fail(String.format("await timed out from  %s - %s", participant1, e.getLocalizedMessage()));
          } finally {
            zkUtils.close();
          }
        });

    Future f2 = pool.submit(
      () -> {
        ZkUtils zkUtils = getZkUtilsWithNewClient(participant2);
        zkUtils.connect();
        Latch latch = new ZkProcessorLatch(latchSize, latchId, participant2, zkUtils);
        latch.countDown();
        try {
          latch.await(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          Assert.fail(String.format("await timed out from  %s - %s", participant2, e.getLocalizedMessage()));
        } finally {
          zkUtils.close();
        }
      });

    try {
      f1.get(30, TimeUnit.SECONDS);
      f2.get(30, TimeUnit.SECONDS);
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

  @Test
  public void testLatchSizeN() {
    final int latchSize = 1;
    final String latchId = "testLatchSizeN";

    ExecutorService pool = Executors.newFixedThreadPool(3);
    Future f1 = pool.submit(
      () -> {
        String participant1 = "participant1";
        ZkUtils zkUtils = getZkUtilsWithNewClient(participant1);
        zkUtils.connect();
        Latch latch = new ZkProcessorLatch(latchSize, latchId, participant1, zkUtils);
        latch.countDown();
        try {
          latch.await(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          Assert.fail(String.format("await timed out from  %s - %s", participant1, e.getLocalizedMessage()));
        } finally {
          zkUtils.close();
        }
      });
    Future f2 = pool.submit(
      () -> {
        String participant2 = "participant2";
        ZkUtils zkUtils = getZkUtilsWithNewClient(participant2);
        zkUtils.connect();
        Latch latch = new ZkProcessorLatch(latchSize, latchId, participant2, zkUtils);
        latch.countDown();
        try {
          latch.await(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          Assert.fail(String.format("await timed out from  %s - %s", participant2, e.getLocalizedMessage()));
        } finally {
          zkUtils.close();
        }
      });

    Future f3 = pool.submit(
      () -> {
        String participant3 = "participant3";
        ZkUtils zkUtils = getZkUtilsWithNewClient(participant3);
        zkUtils.connect();
        Latch latch = new ZkProcessorLatch(latchSize, latchId, participant3, zkUtils);
        latch.countDown();
        try {
          latch.await(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          Assert.fail(String.format("await timed out from  %s - %s", participant3, e.getLocalizedMessage()));
        } finally {
          zkUtils.close();
        }
      });

    try {
      f1.get(30, TimeUnit.SECONDS);
      f2.get(30, TimeUnit.SECONDS);
      f3.get(30, TimeUnit.SECONDS);
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
      Assert.assertEquals(3, latchParticipants.size());
    } catch (Exception e) {
      Assert.fail("Failed to read the latch status from ZK directly" + e.getLocalizedMessage());
    }
  }

  @Test
  public void testLatchExpires() {
    final String latchId = "testLatchExpires";
    final int latchSize = 3;

    Latch latch = new ZkProcessorLatch(latchSize, latchId, "test", testZkUtils);
    try {
      latch.countDown();
      latch.await(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // expected
    } catch (Exception e) {
      Assert.fail(String.format("Expected only TimeoutException! Received %s", e));
    }

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
