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
import java.util.concurrent.TimeoutException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationService;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.ProcessorLatch;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestZkProcessorLatch {
  private EmbeddedZookeeper zkServer = null;
  private ExecutorService pool;
  private String zkConnectionString;
  private final CoordinationServiceFactory factory = new ZkCoordinationServiceFactory();
  private CoordinationService coordinationService;

  @BeforeClass
  public void setup() throws InterruptedException {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();

    zkConnectionString = "localhost:" + zkServer.getPort();
    System.out.println("ZK port = " + zkServer.getPort());

    String groupId = "group1";
    String processorId = "p1";
    Map<String, String> map = new HashMap<>();
    map.put(ZkConfig.ZK_CONNECT, zkConnectionString);
    Config config = new MapConfig(map);


    coordinationService = factory.getCoordinationService(groupId, processorId, config);
    coordinationService.start();
    coordinationService.reset();

    pool = Executors.newFixedThreadPool(3);
  }

  @AfterClass
  public void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testSingleLatch() {
    System.out.println("Started 1");
    int latchSize = 1;
    String latchId = "l2";

    Future f1 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });

    try {
      f1.get(30000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("failed to get future." + e.getLocalizedMessage());
    }

  }

  @Test
  public void testNSizeLatch() {
    System.out.println("Started N");
    String latchId = "l1";
    int latchSize = 3;

    Future f1 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out " + e.getLocalizedMessage());
      }
    });
    Future f2 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });
    Future f3 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });

    try {
      f1.get(300, TimeUnit.MILLISECONDS);
      f2.get(300, TimeUnit.MILLISECONDS);
      f3.get(300, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("failed to get future. " + e.getLocalizedMessage());
    }
  }

  @Test
  public void testLatchExpires() {
    System.out.println("Started expiring");
    String latchId = "l4";

    int latchSize = 3;

    Future f1 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });
    Future f2 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });
    Future f3 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      // This processor never completes its task
      //latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });

    try {
      f1.get(300, TimeUnit.MILLISECONDS);
      f2.get(300, TimeUnit.MILLISECONDS);
      f3.get(300, TimeUnit.MILLISECONDS);
      Assert.fail("Latch should've timeout.");
    } catch (Exception e) {
      f1.cancel(true);
      f2.cancel(true);
      f3.cancel(true);
      // expected
    }
  }

  @Test
  public void testSingleCountdown() {
    System.out.println("Started single countdown");
     String latchId = "l1";
    int latchSize = 3;

    // Only one thread invokes countDown
    Future f1 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      latch.countDown();
      TestZkUtils.sleepMs(100);
      latch.countDown();
      TestZkUtils.sleepMs(100);
      latch.countDown();
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });
    Future f2 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });
    Future f3 = pool.submit(() -> {
      ProcessorLatch latch = coordinationService.getLatch(latchSize, latchId);
      try {
        latch.await(TimeUnit.MILLISECONDS, 100000);
      } catch (TimeoutException e) {
        Assert.fail("await timed out. " + e.getLocalizedMessage());
      }
    });

    try {
      f1.get(600, TimeUnit.MILLISECONDS);
      f2.get(600, TimeUnit.MILLISECONDS);
      f3.get(600, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      Assert.fail("Failed to get.");
    }
  }
}
