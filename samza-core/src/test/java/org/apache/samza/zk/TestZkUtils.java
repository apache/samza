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

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZkUtils {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private ZkConnection zkConnection = null;
  private ZkClient zkClient = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;

  @BeforeClass
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @Before
  public void testSetup() {
    try {
      zkClient = new ZkClient(
          new ZkConnection("127.0.0.1:" + zkServer.getPort(), SESSION_TIMEOUT_MS),
          CONNECTION_TIMEOUT_MS);
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
    try {
      zkClient.createPersistent(KEY_BUILDER.getProcessorsPath(), true);
    } catch (ZkNodeExistsException e) {
      // Do nothing
    }
  }


  @After
  public void testTeardown() {
    zkClient.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testRegisterProcessorId() {
    ZkUtils utils = new ZkUtils(
        KEY_BUILDER,
        zkClient,
        SESSION_TIMEOUT_MS);
    utils.connect();
    String assignedPath = utils.registerProcessorAndGetId("0.0.0.0");
    Assert.assertTrue(assignedPath.startsWith(KEY_BUILDER.getProcessorsPath()));

    // Calling registerProcessorId again should return the same ephemeralPath as long as the session is valid
    Assert.assertTrue(utils.registerProcessorAndGetId("0.0.0.0").equals(assignedPath));

    utils.close();
  }

  @Test
  public void testGetActiveProcessors() {
    ZkUtils utils = new ZkUtils(
        KEY_BUILDER,
        zkClient,
        SESSION_TIMEOUT_MS);
    utils.connect();

    Assert.assertEquals(0, utils.getSortedActiveProcessors().size());
    utils.registerProcessorAndGetId("processorData");

    Assert.assertEquals(1, utils.getSortedActiveProcessors().size());

    utils.close();
  }

}
