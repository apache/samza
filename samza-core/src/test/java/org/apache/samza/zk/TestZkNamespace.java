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

import com.google.common.base.Strings;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.samza.SamzaException;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZkNamespace {
  private static EmbeddedZookeeper zkServer = null;
  private ZkClient zkClient = null;
  private ZkClient zkClient1 = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;

  @BeforeClass
  public static void setup()
      throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  // for these tests we need to connect to zk multiple times
  private void initZk(String zkConnect) {
    try {
      zkClient = new ZkClient(new ZkConnection(zkConnect, SESSION_TIMEOUT_MS), CONNECTION_TIMEOUT_MS);
    } catch (Exception e) {
      Assert.fail("Client connection setup failed for connect + " + zkConnect + ": " + e);
    }
  }

  private void tearDownZk() {
    if (zkClient != null) {
      zkClient.close();
    }

    if (zkClient1 != null) {
      zkClient1.close();
    }
  }

  // create namespace for zk before accessing it, thus using a separate client
  private void createNamespace(String pathToCreate) {
    if (Strings.isNullOrEmpty(pathToCreate)) {
      return;
    }

    String zkConnect = "127.0.0.1:" + zkServer.getPort();
    try {
      zkClient1 = new ZkClient(new ZkConnection(zkConnect, SESSION_TIMEOUT_MS), CONNECTION_TIMEOUT_MS);
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    zkClient1.createPersistent(pathToCreate, true);
  }

  // auxiliary method - create connection, validate the zk connection (should fail, because namespace does not exist
  private void testFailIfNameSpaceMissing(String zkNameSpace) {
    String zkConnect = "127.0.0.1:" + zkServer.getPort() + zkNameSpace;
    initZk(zkConnect);
    ZkCoordinationServiceFactory.validateZkNameSpace(zkConnect, zkClient);
  }

  // create namespace, create connection, validate the connection
  private void testDoNotFailIfNameSpacePresent(String zkNameSpace) {
    String zkConnect = "127.0.0.1:" + zkServer.getPort() + zkNameSpace;
    createNamespace(zkNameSpace);
    initZk(zkConnect);
    ZkCoordinationServiceFactory.validateZkNameSpace(zkConnect, zkClient);

    zkClient.createPersistent("/test");
    zkClient.createPersistent("/test/test1");

    // test if the new root exists
    Assert.assertTrue(zkClient.exists("/"));
    Assert.assertTrue(zkClient.exists("/test"));
    Assert.assertTrue(zkClient.exists("/test/test1"));
  }

  @Test
  public void testValidateFailZkNameSpace() {
    try {
      testFailIfNameSpaceMissing("/zkNameSpace");
      Assert.fail("Should fail with exception, because namespace doesn't exist");
    } catch (SamzaException e) {
      // expected
    } finally {
      tearDownZk();
    }

    try {
      testFailIfNameSpaceMissing("/zkNameSpace/xyz");
      Assert.fail("Should fail with exception, because namespace doesn't exist");
    } catch (SamzaException e) {
      // expected
    } finally {
      tearDownZk();
    }

    // should succeed, because no namespace provided
    testFailIfNameSpaceMissing("");
    tearDownZk();
  }

  //@Test
  public void testValidateNotFailZkNameSpace() {
    // now positive tests - with existing namespace
    testDoNotFailIfNameSpacePresent("/zkNameSpace1");

    testDoNotFailIfNameSpacePresent("/zkNameSpace1/xyz1");

    testDoNotFailIfNameSpacePresent("");

    teardown();
  }
}

