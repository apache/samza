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

import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZkBarrierForVersionUpgrade {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
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
    testZkConnectionString = "localhost:" + zkServer.getPort();
    try {
      testZkUtils = getZkUtilsWithNewClient();
    } catch (Exception e) {
      Assert.fail("Client connection setup failed. Aborting tests..");
    }
  }

  @After
  public void testTeardown() {
    testZkUtils.deleteRoot();
    testZkUtils.close();
    testZkUtils = null;
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testZkBarrierForVersionUpgrade() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();
    ZkBarrierForVersionUpgrade barrier = new ZkBarrierForVersionUpgrade(testZkUtils, debounceTimer);
    String ver = "1";
    List<String> processors = new ArrayList<String>();
    processors.add("p1");
    processors.add("p2");

    class Status {
      boolean p1 = false;
      boolean p2 = false;
    }
    final Status s = new Status();

    barrier.leaderStartBarrier(ver, processors);

    barrier.waitForBarrier(ver, "p1", new Runnable() {
      @Override
      public void run() {
        s.p1 = true;
      }
    });

    barrier.waitForBarrier(ver, "p2", new Runnable() {
      @Override
      public void run() {
        s.p2 = true;
      }
    });

    Assert.assertTrue(TestZkUtils.testWithDelayBackOff(() -> s.p1 && s.p2, 2, 100));
  }

  @Test
  public void testNegativeZkBarrierForVersionUpgrade() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();
    ZkBarrierForVersionUpgrade barrier = new ZkBarrierForVersionUpgrade(testZkUtils, debounceTimer);
    String ver = "1";
    List<String> processors = new ArrayList<String>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3");

    class Status {
      boolean p1 = false;
      boolean p2 = false;
      boolean p3 = false;
    }
    final Status s = new Status();

    barrier.leaderStartBarrier(ver, processors);

    barrier.waitForBarrier(ver, "p1", new Runnable() {
      @Override
      public void run() {
        s.p1 = true;
      }
    });

    barrier.waitForBarrier(ver, "p2", new Runnable() {
      @Override
      public void run() {
        s.p2 = true;
      }
    });

    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> s.p1 && s.p2 && s.p3, 2, 100));

  }


  private ZkUtils getZkUtilsWithNewClient() {
    ZkConnection zkConnection = ZkUtils.createZkConnection(testZkConnectionString, SESSION_TIMEOUT_MS);
    return new ZkUtils(
        KEY_BUILDER,
        ZkUtils.createZkClient(zkConnection, CONNECTION_TIMEOUT_MS),
        CONNECTION_TIMEOUT_MS);
  }
}
