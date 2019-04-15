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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.DistributedDataWatcher;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;


public class TestZkDistributedDataAccess {
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private ZkUtils zkUtils;
  private ZkUtils zkUtils1;
  private DistributedDataWatcher mockDataWatcher = mock(DistributedDataWatcher.class);


  @BeforeClass
  public static void test() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = String.format("127.0.0.1:%d", zkServer.getPort());
  }

  @Before
  public void testSetup() {
    ZkClient zkClient = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils = new ZkUtils(new ZkKeyBuilder("group1"), zkClient, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
    ZkClient zkClient1 = new ZkClient(testZkConnectionString, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    this.zkUtils1 = new ZkUtils(new ZkKeyBuilder("group1"), zkClient1, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
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

  @Test
  public void testZkDistributedDataAccess() {
    ZkDistributedDataAccess dataAccess = new ZkDistributedDataAccess(zkUtils);
    ZkDistributedDataAccess dataAccess1 = new ZkDistributedDataAccess(zkUtils1);

    String data = "FAKE_DATA";
    String key = "FAKE_KEY";

    dataAccess.writeData(key, data, mockDataWatcher);
    String readData = (String) dataAccess1.readData(key, mockDataWatcher);

    assertEquals("Data read by second processor is not the same as that written by the first.", data, readData);
  }

  @Test
  public void testZkDistributedDataAccessPersistent() {
    ZkDistributedDataAccess dataAccess = new ZkDistributedDataAccess(zkUtils);
    ZkDistributedDataAccess dataAccess1 = new ZkDistributedDataAccess(zkUtils1);

    String data = "FAKE_DATA";
    String key = "FAKE_KEY1";

    dataAccess.writeData(key, data, mockDataWatcher);
    zkUtils.close();

    String readData = (String) dataAccess1.readData(key, mockDataWatcher);

    assertEquals("Data read by second processor after first one closed is not the same as that written by the first.", data, readData);
  }

  @Test
  public void testZkDistributedDataAccessOverwrite() {
    ZkDistributedDataAccess dataAccess = new ZkDistributedDataAccess(zkUtils);
    ZkDistributedDataAccess dataAccess1 = new ZkDistributedDataAccess(zkUtils1);
    String data = "FAKE_DATA";
    String key = "FAKE_KEY2";

    dataAccess.writeData(key, data, mockDataWatcher);
    String readData = (String) dataAccess1.readData(key, mockDataWatcher);

    assertEquals("Data read by second processor is not the same as that written by the first.", data, readData);

    String newData = "NEW_FAKE_DATA";

    dataAccess.writeData(key, newData, mockDataWatcher);

    String newlyReadData = (String) dataAccess1.readData(key, mockDataWatcher);
    assertEquals("Data read by second processor after overwrite is not the same as the overwritten data.", newData, newlyReadData);
  }

  @Test
  public void testZkDistributedDataAccessWatchers() throws InterruptedException {
    final CountDownLatch watcherInvoked = new CountDownLatch(1);
    String key = "FAKE_KEY3";
    String newData = "NEW_FAKE_DATA";
    ZkDistributedDataAccess dataAccess = new ZkDistributedDataAccess(zkUtils);
    ZkDistributedDataAccess dataAccess1 = new ZkDistributedDataAccess(zkUtils1);
    DistributedDataWatcher watcher = mock(DistributedDataWatcher.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        watcherInvoked.countDown();
        return null;
      }
    }).when(watcher).handleDataChange(newData);
    DistributedDataWatcher watcher1 = mock(DistributedDataWatcher.class);

    dataAccess.readData(key, watcher);
    dataAccess1.writeData(key, newData, watcher1);
    //ensure watcher.handleDataChange called before verfying
    try {
      if (!watcherInvoked.await(30000, TimeUnit.MILLISECONDS)) {
        fail("Timed out while waiting for the handleDataChange callbakc to be invoked.");
      }
    } catch (InterruptedException e) {
      fail("Got interrupted while waiting for the handleDataChange callbakc to be invoked.");
    }
    // watcher of the reader notified of the write
    verify(watcher, times(1)).handleDataChange(newData);
  }
}
