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

import com.google.common.collect.ImmutableList;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.zk.ZkBarrierForVersionUpgrade.State;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.*;

public class TestZkBarrierForVersionUpgrade {
  private static final String BARRIER_VERSION = "1";

  private final ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime("TEST_PROCESSOR_ID");
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private ZkUtils zkUtils;
  private ZkUtils zkUtils1;

  @BeforeClass
  public static void test() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = String.format("127.0.0.1:%d", zkServer.getPort());
  }

  @Before
  public void testSetup() {
    ZkClient zkClient = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils = new ZkUtils(new ZkKeyBuilder("group1"), zkClient, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
    ZkClient zkClient1 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils1 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient1, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
  }

  @After
  public void testTearDown() {
    zkUtils.close();
    zkUtils1.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  static class TestZkBarrierListener implements ZkBarrierListener {

    private final CountDownLatch stateChangedLatch;
    private final State expectedState;

    TestZkBarrierListener(CountDownLatch stateChangedLatch, State expectedState) {
      this.stateChangedLatch = stateChangedLatch;
      this.expectedState = expectedState;
    }

    @Override
    public void onBarrierCreated(String version) {}

    @Override
    public void onBarrierStateChanged(String version, State state) {
      if (state.equals(expectedState)) {
        stateChangedLatch.countDown();
      }
    }

    @Override
    public void onBarrierError(String version, Throwable t) {}
  }

  @Test
  public void testZkBarrierForVersionUpgrade() {
    String barrierId = String.format("%s/%s", zkUtils1.getKeyBuilder().getRootPath(), RandomStringUtils.randomAlphabetic(4));

    List<String> processors = ImmutableList.of("p1", "p2");

    CountDownLatch latch = new CountDownLatch(2);
    TestZkBarrierListener listener = new TestZkBarrierListener(latch, State.DONE);

    ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, listener, debounceTimer);
    ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils1, listener, debounceTimer);

    processor1Barrier.create(BARRIER_VERSION, processors);

    State barrierState = zkUtils.getZkClient().readData(barrierId + "/barrier_1/barrier_state");
    assertEquals(State.NEW, barrierState);

    processor1Barrier.join(BARRIER_VERSION, "p1");
    processor2Barrier.join(BARRIER_VERSION, "p2");

    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue("Barrier failed to complete within test timeout.", result);

    List<String> children = zkUtils.getZkClient().getChildren(barrierId + "/barrier_1/barrier_participants");
    barrierState = zkUtils.getZkClient().readData(barrierId + "/barrier_1/barrier_state");
    assertEquals(State.DONE, barrierState);
    assertNotNull(children);
    assertEquals("Unexpected barrier state. Didn't find two processors.", 2, children.size());
    assertEquals("Unexpected barrier state. Didn't find the expected members.", processors, children);
  }

  @Test
  public void testZkBarrierForVersionUpgradeWithTimeOut() {
    String barrierId = String.format("%s/%s", zkUtils1.getKeyBuilder().getRootPath(), RandomStringUtils.randomAlphabetic(4));
    List<String> processors = ImmutableList.of("p1", "p2", "p3");

    CountDownLatch latch = new CountDownLatch(2);
    TestZkBarrierListener listener = new TestZkBarrierListener(latch, State.TIMED_OUT);

    ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, listener, debounceTimer);
    ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils1, listener, debounceTimer);

    processor1Barrier.create(BARRIER_VERSION, processors);

    processor1Barrier.join(BARRIER_VERSION, "p1");
    processor2Barrier.join(BARRIER_VERSION, "p2");

    processor1Barrier.expire(BARRIER_VERSION);
    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue("Barrier Timeout test failed to complete within test timeout.", result);

    List<String> children = zkUtils.getZkClient().getChildren(barrierId + "/barrier_1/barrier_participants");
    State barrierState = zkUtils.getZkClient().readData(barrierId + "/barrier_1/barrier_state");
    assertEquals(State.TIMED_OUT, barrierState);
    assertNotNull(children);
    assertEquals("Unexpected barrier state. Didn't find two processors.", 2, children.size());
    assertEquals("Unexpected barrier state. Didn't find the expected members.", ImmutableList.of("p1", "p2"), children);
  }

  @Test
  public void testShouldDiscardBarrierUpdateEventsAfterABarrierIsMarkedAsDone() {
    String barrierId = String.format("%s/%s", zkUtils1.getKeyBuilder().getRootPath(), RandomStringUtils.randomAlphabetic(4));
    List<String> processors = ImmutableList.of("p1", "p2");

    CountDownLatch latch = new CountDownLatch(2);
    TestZkBarrierListener listener = new TestZkBarrierListener(latch, State.DONE);
    ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, listener, debounceTimer);
    ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils1, listener, debounceTimer);

    processor1Barrier.create(BARRIER_VERSION, processors);

    processor1Barrier.join(BARRIER_VERSION, "p1");
    processor2Barrier.join(BARRIER_VERSION, "p2");

    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue("Barrier Timeout test failed to complete within test timeout.", result);

    processor1Barrier.expire(BARRIER_VERSION);

    State barrierState = zkUtils.getZkClient().readData(barrierId + "/barrier_1/barrier_state");
    assertEquals(State.DONE, barrierState);
  }

  @Test
  public void testShouldDiscardBarrierUpdateEventsAfterABarrierIsMarkedAsTimedOut() {
    String barrierId = String.format("%s/%s", zkUtils1.getKeyBuilder().getRootPath(), RandomStringUtils.randomAlphabetic(4));
    List<String> processors = ImmutableList.of("p1", "p2", "p3");

    CountDownLatch latch = new CountDownLatch(2);
    TestZkBarrierListener listener = new TestZkBarrierListener(latch, State.TIMED_OUT);
    ZkBarrierForVersionUpgrade processor1Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils, listener, debounceTimer);
    ZkBarrierForVersionUpgrade processor2Barrier = new ZkBarrierForVersionUpgrade(barrierId, zkUtils1, listener, debounceTimer);

    processor1Barrier.create(BARRIER_VERSION, processors);

    processor1Barrier.join(BARRIER_VERSION, "p1");
    processor2Barrier.join(BARRIER_VERSION, "p2");

    processor1Barrier.expire(BARRIER_VERSION);

    boolean result = false;
    try {
      result = latch.await(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue("Barrier Timeout test failed to complete within test timeout.", result);


    processor1Barrier.join(BARRIER_VERSION, "p3");

    State barrierState = zkUtils.getZkClient().readData(barrierId + "/barrier_1/barrier_state");
    assertEquals(State.TIMED_OUT, barrierState);
  }
}
