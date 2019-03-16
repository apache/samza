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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.DistributedReadWriteLock.AccessType;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;

import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.*;

public class TestZkDistributedReadWriteLock {
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
    String readWriteLockPath = String.format("%s/readWriteLock-%s", zkUtils1.getKeyBuilder().getRootPath(), lockId);
    return zkUtils.getZkClient().getChildren(readWriteLockPath + "/participants");
  }

  private List<String> getProcessors(ZkUtils zkUtils, String lockId) {
    String readWriteLockPath = String.format("%s/readWriteLock-%s", zkUtils1.getKeyBuilder().getRootPath(), lockId);
    return zkUtils.getZkClient().getChildren(readWriteLockPath + "/processors");
  }

  /**
   * Test a happy path -- one processor locks and unlocks and then a second processor locks and unlocks
   * Ensure that firs processor gets WRITE second processor gets READ
   * Ensure that active participants and active processors list is maintained correctly during locks and unlocks
   * Ensure that all ephemeral nodes are deleted upon cleanState.
   */
  @Test
  public void testZkDistributedReadWriteLock() {
    String lockId = "FAKE_LOCK_ID";
    ZkDistributedReadWriteLock readWriteLock1 = new ZkDistributedReadWriteLock("p1", zkUtils1, lockId);
    ZkDistributedReadWriteLock readWriteLock2 = new ZkDistributedReadWriteLock("p2", zkUtils2, lockId);


    assertEquals("ReadWrite Lock has participants before any processor tried to lock.", 0, getParticipants(zkUtils1, lockId).size());

    try {
      AccessType accessType1 = readWriteLock1.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("ReadWrite Lock does not have 1 participant after first processor tries to lock.", 1, getParticipants(zkUtils1, lockId).size());
      assertEquals("ReadWrite Lock does not have 1 active processor after first processor tries to lock.", 1, getProcessors(zkUtils1, lockId).size());
      assertEquals("First processor to acquire ReadWrite Lock did not get WRITE access.", AccessType.WRITE, accessType1);
      readWriteLock1.unlock();
      AccessType accessType2 = readWriteLock2.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("ReadWrite Lock does not have 1 participant after second processor tries to lock.", 1, getParticipants(zkUtils2, lockId).size());
      assertEquals("ReadWrite Lock does not have 2 active processors after second processor tries to lock.", 2, getProcessors(zkUtils2, lockId).size());
      assertEquals("Second processor to acquire ReadWrite Lock did not get READ access.", AccessType.READ, accessType2);
      readWriteLock2.unlock();
      assertEquals("ReadWrite Lock has participants after both processors have unlocked.", 0, getParticipants(zkUtils1, lockId).size());
      assertEquals("ReadWrite Lock does not have 2 active processors after both processors have unlocked.", 2, getProcessors(zkUtils1, lockId).size());
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
    readWriteLock1.cleanState();
    readWriteLock2.cleanState();
    assertEquals("ReadWrite Lock has active processors after both processors have cleaned state of lock.", 0, getProcessors(zkUtils1, lockId).size());
  }

  /**
   * Test one processor dying before unlocking and second processor locking after that
   * Ensure first processor got WRITE and second processor also gets WRITE
   */
  @Test
  public void testLockWithFirstProcessorClosing() {
    // first processor dies before unlock then second processor should get WRITE access
    String lockId = "FAKE_LOCK_ID_1";
    ZkDistributedReadWriteLock readWriteLock1 = new ZkDistributedReadWriteLock("p1", zkUtils1, lockId);
    ZkDistributedReadWriteLock readWriteLock2 = new ZkDistributedReadWriteLock("p2", zkUtils2, lockId);

    try {
      AccessType accessType1 = readWriteLock1.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("First processor to acquire ReadWrite Lock did not get WRITE access.", AccessType.WRITE, accessType1);
      // first processor dies before unlock
      zkUtils1.close();

      AccessType accessType2 = readWriteLock2.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("Second processor to acquire Lock after first guy closed did not get WRITE access.", AccessType.WRITE, accessType2);
      readWriteLock2.unlock();

      assertEquals("ReadWrite Lock has participants after both processors have unlocked.", 0, getParticipants(zkUtils2, lockId).size());
      assertEquals("ReadWrite Lock does not have 1 active processor after second processor unlocked.", 1, getProcessors(zkUtils2, lockId).size());
    } catch (TimeoutException e) {
      e.printStackTrace();
    }

    readWriteLock2.cleanState();
    assertEquals("ReadWrite Lock has active processors after both processors have cleaned state of lock.", 0, getProcessors(zkUtils2, lockId).size());
  }

  /**
   * Test lock with two groups
   * first group of processors dies after acquiring locks and before unlocking
   * second group of processors gets WRITE access first and then READ access.
   */
  @Test
  public void testLockWithTwoGroups() {
    String lockId = "FAKE_LOCK_ID_2";
    ZkDistributedReadWriteLock readWriteLock1 = new ZkDistributedReadWriteLock("p1", zkUtils1, lockId);
    ZkDistributedReadWriteLock readWriteLock2 = new ZkDistributedReadWriteLock("p2", zkUtils2, lockId);

    try {
      AccessType accessType1 = readWriteLock1.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("First processor in first group to acquire ReadWrite Lock did not get WRITE access.", AccessType.WRITE, accessType1);
      // first processor dies before unlock -- simulating via utils close.
      zkUtils1.close();

      AccessType accessType2 = readWriteLock2.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("Second processor in first group to acquire ReadWrite Lock did not get READ access.", AccessType.WRITE, accessType2);
      // second processor dies before unlock -- simulating via utils close.
      zkUtils2.close();

      ZkClient zkClient3 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
      ZkUtils zkUtils3 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient3, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
      ZkClient zkClient4 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
      ZkUtils zkUtils4 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient4, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
      ZkDistributedReadWriteLock readWriteLock3 = new ZkDistributedReadWriteLock("p3", zkUtils3, lockId);
      ZkDistributedReadWriteLock readWriteLock4 = new ZkDistributedReadWriteLock("p4", zkUtils4, lockId);


      assertEquals("ReadWrite Lock has participants after both processors of first group have died.", 0, getParticipants(zkUtils3, lockId).size());
      assertEquals("ReadWrite Lock has active processors after both processors of first group have died.", 0, getProcessors(zkUtils3, lockId).size());

      AccessType accessType3 = readWriteLock3.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("First processor in second group to acquire ReadWrite Lock did not get WRITE access.", AccessType.WRITE, accessType3);
      readWriteLock3.unlock();
      AccessType accessType4 = readWriteLock4.lock(10000, TimeUnit.MILLISECONDS);
      assertEquals("Second processor in second group to acquire ReadWrite Lock did not get READ access.", AccessType.READ, accessType4);
      readWriteLock4.unlock();

    } catch (TimeoutException e) {
      e.printStackTrace();
    }
  }
}

