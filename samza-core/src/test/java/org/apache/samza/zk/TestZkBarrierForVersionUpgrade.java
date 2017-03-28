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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZkBarrierForVersionUpgrade {
  private static EmbeddedZookeeper zkServer = null;
  private static String testZkConnectionString = null;
  private static CoordinationUtils coordinationUtils;


  @BeforeClass
  public static void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
    testZkConnectionString = "localhost:" + zkServer.getPort();
  }

  @Before
  public void testSetup() {
    String groupId = "group1";
    String processorId = "p1";
    Map<String, String> map = new HashMap<>();
    map.put(ZkConfig.ZK_CONNECT, testZkConnectionString);
    map.put(ZkConfig.ZK_BARRIER_TIMEOUT_MS, "200");
    Config config = new MapConfig(map);

    CoordinationServiceFactory serviceFactory = new ZkCoordinationServiceFactory();
    coordinationUtils = serviceFactory.getCoordinationService(groupId, processorId, config);
    coordinationUtils.reset();
  }

  @After
  public void testTearDown() {
    coordinationUtils.reset();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testZkBarrierForVersionUpgrade() {
    String barrierId = "b1";
    String ver = "1";
    List<String> processors = new ArrayList<String>();
    processors.add("p1");
    processors.add("p2");

    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(barrierId);

    class Status {
      boolean p1 = false;
      boolean p2 = false;
    }
    final Status s = new Status();

    barrier.start(ver, processors);

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

    String barrierId = "b1";
    String ver = "1";
    List<String> processors = new ArrayList<String>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3");

    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(barrierId);

    class Status {
      boolean p1 = false;
      boolean p2 = false;
      boolean p3 = false;
    }
    final Status s = new Status();

    barrier.start(ver, processors);

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

  @Test
  public void testZkBarrierForVersionUpgradeWithTimeOut() {
    String barrierId = "b1";
    String ver = "1";
    List<String> processors = new ArrayList<String>();
    processors.add("p1");
    processors.add("p2");
    processors.add("p3");

    BarrierForVersionUpgrade barrier = coordinationUtils.getBarrier(barrierId);

    class Status {
      boolean p1 = false;
      boolean p2 = false;
      boolean p3 = false;
    }
    final Status s = new Status();

    barrier.start(ver, processors);

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

    // this node will join "too late"
    barrier.waitForBarrier(ver, "p3", new Runnable() {
      @Override
      public void run() {
        TestZkUtils.sleepMs(300);
        s.p3 = true;
      }
    });
    Assert.assertFalse(TestZkUtils.testWithDelayBackOff(() -> s.p1 && s.p2 && s.p3, 2, 400));

  }
}
