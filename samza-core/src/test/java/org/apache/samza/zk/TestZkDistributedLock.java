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

import java.time.Duration;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;

import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.*;

public class TestZkDistributedLock {
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

  private List<String> getParticipants(ZkUtils zkUtils, String lockId) {
    String lockPath = String.format("%s/lock_%s", zkUtils1.getKeyBuilder().getRootPath(), lockId);
    return zkUtils.getZkClient().getChildren(lockPath);
  }

  @Test
  public void testLockSingleProcessor() {
    String lockId = "FAKE_LOCK_ID_1";
    ZkDistributedLock lock1 = new ZkDistributedLock("p1", zkUtils1, lockId);


    assertEquals("Lock has participants before any processor tried to lock.", 0, getParticipants(zkUtils1, lockId).size());

    boolean lock1Status = lock1.lock(Duration.ofMillis(10000));
    assertEquals("Lock does not have 1 participant after first processor tries to lock.", 1, getParticipants(zkUtils1, lockId).size());
    assertEquals("1st processor requesting to lock did not acquire the lock.", true, lock1Status);
    lock1.unlock();
    assertEquals("Lock does have 1 participant after first processor tries to unlock.", 0, getParticipants(zkUtils1, lockId).size());
  }

  @Test
  public void testLockTwoProcessors() {
    // second processor should acquire lock after first one unlocks
    String lockId = "FAKE_LOCK_ID_2";
    ZkDistributedLock lock1 = new ZkDistributedLock("p1", zkUtils1, lockId);
    ZkDistributedLock lock2 = new ZkDistributedLock("p2", zkUtils2, lockId);

    assertEquals("Lock has participants before any processor tried to lock.", 0, getParticipants(zkUtils1, lockId).size());

    boolean lock1Status = lock1.lock(Duration.ofMillis(10000));
    assertEquals("First processor requesting to lock did not acquire the lock.", true, lock1Status);
    lock1.unlock();
    boolean lock2Status = lock2.lock(Duration.ofMillis(10000));
    assertEquals("Second processor requesting to lock did not acquire the lock.", true, lock2Status);
    lock2.unlock();
    assertEquals("Lock does have participants after processors unlocked.", 0, getParticipants(zkUtils1, lockId).size());
  }

  @Test
  public void testLockFirstProcessorClosing() {
    // first processor dies before unlock then second processor should acquire
    String lockId = "FAKE_LOCK_ID_3";
    ZkDistributedLock lock1 = new ZkDistributedLock("p1", zkUtils1, lockId);
    ZkDistributedLock lock2 = new ZkDistributedLock("p2", zkUtils2, lockId);


    assertEquals("Lock has participants before any processor tried to lock!", 0, getParticipants(zkUtils1, lockId).size());

    boolean lock1Status = lock1.lock(Duration.ofMillis(10000));
    assertEquals("First processor requesting to lock did not acquire the lock.", true, lock1Status);
    // first processor dies before unlock
    zkUtils1.close();

    boolean lock2Status = lock2.lock(Duration.ofMillis(10000));
    assertEquals("Second processor requesting to lock did not acquire the lock.", true, lock2Status);
    lock2.unlock();
    assertEquals("Lock does have participants after processors unlocked.", 0, getParticipants(zkUtils2, lockId).size());
  }
}
