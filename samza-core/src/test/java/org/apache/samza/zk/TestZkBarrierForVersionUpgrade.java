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

import junit.framework.Assert;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.BarrierForVersionUpgradeListener;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class TestZkBarrierForVersionUpgrade {
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private ZkUtils zkUtils;

  @BeforeClass
  public static void test() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = "127.0.0.1:" + zkServer.getPort();
  }

  @Before
  public void testSetup() {
    ZkClient zkClient = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils = new ZkUtils(new ZkKeyBuilder("group1"), zkClient, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  @After
  public void testTearDown() {
    zkServer.teardown();
  }

  // TODO: SAMZA-1193 fix the following flaky test and re-enable it
  @Test
  public void testZkBarrierForVersionUpgrade() {
    String barrierId = zkUtils.getKeyBuilder().getRootPath() + "/b1";
    String ver = "1";
    List<String> processors = new ArrayList<>();
    processors.add("p1");
    processors.add("p2");
    final CountDownLatch latch = new CountDownLatch(2);
    BarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, new BarrierForVersionUpgradeListener() {
      @Override
      public void onBarrierStart(String version) {
        // do nothing
      }

      @Override
      public void onBarrierComplete(String version, BarrierForVersionUpgrade.State barrierState) {
        latch.countDown();
      }

      @Override
      public void onBarrierError(String version, Throwable t) {

      }
    });

    processor1Barrier.start(ver, processors);
    processor1Barrier.joinBarrier(ver, "p1");

    BarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, new BarrierForVersionUpgradeListener() {
      @Override
      public void onBarrierStart(String version) {
        // do nothing
      }

      @Override
      public void onBarrierComplete(String version, BarrierForVersionUpgrade.State barrierState) {
        latch.countDown();
      }

      @Override
      public void onBarrierError(String version, Throwable t) {

      }
    });
    processor2Barrier.joinBarrier(ver, "p2");
    try {
      latch.await(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    try {
      List<String> children = zkUtils.getZkClient().getChildren(barrierId + "/barrier_v1/barrier_processors");
      Assert.assertNotNull(children);
      Assert.assertEquals("Unexpected barrier state. Didn't find two processors.", 2, children.size());
      Assert.assertEquals("Unexpected barrier state. Didn't find the expected members.", processors, children);
    } catch (Exception e) {
      // no-op
    }
  }

 /* @Test
  public void testNegativeZkBarrierForVersionUpgrade() {
    String barrierId = zkUtils.getKeyBuilder().getRootPath() + "/negativeZkBarrierForVersionUpgrade";
    String ver = "1";
    List<String> processors = new ArrayList<>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3");

    BarrierForVersionUpgrade barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, ZkConfig.DEFAULT_BARRIER_TIMEOUT_MS);

    class Status {
      boolean p1 = false;
      boolean p2 = false;
      boolean p3 = false;
    }
    final Status s = new Status();

    barrier.start(ver, processors);

    barrier.waitForBarrier(ver, "p1", () -> s.p1 = true);

    barrier.waitForBarrier(ver, "p2", () -> s.p2 = true);

    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> s.p1 && s.p2 && s.p3, 2, 100));
  }

  @Test
  public void testZkBarrierForVersionUpgradeWithTimeOut() {
    String barrierId = zkUtils.getKeyBuilder().getRootPath() + "/barrierTimeout";
    String ver = "1";
    List<String> processors = new ArrayList<>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3");

    BarrierForVersionUpgrade barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, ZkConfig.DEFAULT_BARRIER_TIMEOUT_MS);

    class Status {
      boolean p1 = false;
      boolean p2 = false;
      boolean p3 = false;
    }
    final Status s = new Status();

    barrier.start(ver, processors);

    barrier.joinBarrier(ver, "p1", () -> s.p1 = true);

    barrier.joinBarrier(ver, "p2", () -> s.p2 = true);

    // this node will join "too late"
    barrier.joinBarrier(ver, "p3", () -> {
      TestZkUtils.sleepMs(300);
      s.p3 = true;
    });
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> s.p1 && s.p2 && s.p3, 2, 400));
  }*/
}
