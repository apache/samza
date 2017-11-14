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
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;

// TODO: Rename this such that it is clear that it is an integration test and NOT unit test
public class TestZkBarrierForVersionUpgrade {
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private ZkUtils zkUtils;
  private ZkUtils zkUtils1;

  //@BeforeClass
  public static void test() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = "127.0.0.1:" + zkServer.getPort();
  }

  //@Before
  public void testSetup() {
    ZkClient zkClient = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils = new ZkUtils(new ZkKeyBuilder("group1"), zkClient, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
    ZkClient zkClient1 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils1 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient1, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
  }

  //@After
  public void testTearDown() {
    zkUtils.close();
    zkUtils1.close();
  }

  //@AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  //@Test
  public void testZkBarrierForVersionUpgrade() {
    String barrierId = zkUtils.getKeyBuilder().getRootPath() + "/b1";
    String ver = "1";
    List<String> processors = new ArrayList<>();
    processors.add("p1");
    processors.add("p2");
    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicInteger stateChangedCalled = new AtomicInteger(0);

    ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, new ZkBarrierListener() {
      @Override
      public void onBarrierCreated(String version) {
      }

      @Override
      public void onBarrierStateChanged(String version, ZkBarrierForVersionUpgrade.State state) {
        if (state.equals(ZkBarrierForVersionUpgrade.State.DONE)) {
          latch.countDown();
          stateChangedCalled.incrementAndGet();
        }
      }

      @Override
      public void onBarrierError(String version, Throwable t) {

      }
    });

    processor1Barrier.create(ver, processors);
    processor1Barrier.join(ver, "p1");

    ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils1, new ZkBarrierListener() {
      @Override
      public void onBarrierCreated(String version) {
      }

      @Override
      public void onBarrierStateChanged(String version, ZkBarrierForVersionUpgrade.State state) {
        if (state.equals(ZkBarrierForVersionUpgrade.State.DONE)) {
          latch.countDown();
          stateChangedCalled.incrementAndGet();
        }
      }

      @Override
      public void onBarrierError(String version, Throwable t) {

      }
    });
    processor2Barrier.join(ver, "p2");

    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertTrue("Barrier failed to complete within test timeout.", result);

    try {
      List<String> children = zkUtils.getZkClient().getChildren(barrierId + "/barrier_v1/barrier_participants");
      Assert.assertNotNull(children);
      Assert.assertEquals("Unexpected barrier state. Didn't find two processors.", 2, children.size());
      Assert.assertEquals("Unexpected barrier state. Didn't find the expected members.", processors, children);
    } catch (Exception e) {
      // no-op
    }
    Assert.assertEquals(2, stateChangedCalled.get());
  }

  //@Test
  public void testZkBarrierForVersionUpgradeWithTimeOut() {
    String barrierId = zkUtils1.getKeyBuilder().getRootPath() + "/barrierTimeout";
    String ver = "1";
    List<String> processors = new ArrayList<>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3"); // Simply to prevent barrier from completion for testing purposes

    final AtomicInteger timeoutStateChangeCalled = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(2);
    final ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(
        barrierId,
        zkUtils,
        new ZkBarrierListener() {
          @Override
          public void onBarrierCreated(String version) {
          }

          @Override
          public void onBarrierStateChanged(String version, ZkBarrierForVersionUpgrade.State state) {
            if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
              timeoutStateChangeCalled.incrementAndGet();
              latch.countDown();
            }
          }

          @Override
          public void onBarrierError(String version, Throwable t) {

          }

        });
    processor1Barrier.create(ver, processors);
    processor1Barrier.join(ver, "p1");

    final ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(
        barrierId,
        zkUtils1,
        new ZkBarrierListener() {
          @Override
          public void onBarrierCreated(String version) {
          }

          @Override
          public void onBarrierStateChanged(String version, ZkBarrierForVersionUpgrade.State state) {
            if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
              timeoutStateChangeCalled.incrementAndGet();
              latch.countDown();
            }
          }

          @Override
          public void onBarrierError(String version, Throwable t) {

          }

        });

    processor2Barrier.join(ver, "p2");

    processor1Barrier.expire(ver);
    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertTrue("Barrier Timeout test failed to complete within test timeout.", result);

    try {
      List<String> children = zkUtils.getZkClient().getChildren(barrierId + "/barrier_v1/barrier_participants");
      Assert.assertNotNull(children);
      Assert.assertEquals("Unexpected barrier state. Didn't find two processors.", 2, children.size());
      Assert.assertEquals("Unexpected barrier state. Didn't find the expected members.", processors, children);
    } catch (Exception e) {
      // no-op
    }
    Assert.assertEquals(2, timeoutStateChangeCalled.get());
  }
}
