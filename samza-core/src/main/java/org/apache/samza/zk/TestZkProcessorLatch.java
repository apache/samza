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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.ProcessorLatch;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestZkProcessorLatch {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private ZkClient zkClient = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;
  private ZkUtils zkUtils;
  private ExecutorService pool = Executors.newFixedThreadPool(3);
  private String zkConnectionString;

  @BeforeSuite
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @BeforeTest
  public void testSetup() {
    try {
      zkConnectionString = "localhost:" + zkServer.getPort();
      zkClient = new ZkClient(
          new ZkConnection(zkConnectionString, SESSION_TIMEOUT_MS),
          CONNECTION_TIMEOUT_MS);
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    try {
      zkClient.createPersistent(KEY_BUILDER.getRootPath(), true);
    } catch (ZkNodeExistsException e) {
      Assert.fail("failed to create zk root");
    }

    zkUtils = new ZkUtils(
        "testProcessorId",
        KEY_BUILDER,
        zkClient,
        SESSION_TIMEOUT_MS);

    zkUtils.connect();

  }


  @AfterTest
  public void testTeardown() {
    zkUtils.close();
    zkClient.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testSingleLatch() {
    String groupId = "group1";
    String processorId = "p1";
    String latchId = "l1";
    Map<String, String> map = new HashMap<>();
    map.put("coordinator.zk.connect", zkConnectionString);
    Config config = new MapConfig(map);

    int latchSize = 3;
    final CoordinationServiceFactory factory = new ZkCoordinationServiceFactory(groupId, processorId, config);


    Future f1 = pool.submit(() -> {
      ProcessorLatch latch = factory.getCoordinationService(groupId).getLatch(latchSize, latchId);
      latch.countDown();
      latch.await(TimeUnit.MILLISECONDS, 100000);
    });
    Future f2 = pool.submit(() -> {
      ProcessorLatch latch = factory.getCoordinationService(groupId).getLatch(latchSize, latchId);
      latch.countDown();
      latch.await(TimeUnit.MILLISECONDS, 100000);
    });
    Future f3 = pool.submit(() -> {
      ProcessorLatch latch = factory.getCoordinationService(groupId).getLatch(latchSize, latchId);
      latch.countDown();
      latch.await(TimeUnit.MILLISECONDS, 100000);
    });

    try {
      f1.get(300, TimeUnit.MILLISECONDS);
      f2.get(300, TimeUnit.MILLISECONDS);
      f3.get(300, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("failed to get", e);
    }
  }
}
