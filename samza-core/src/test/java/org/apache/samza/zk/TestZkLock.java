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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import junit.framework.Assert;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.zk.ZkCoordinationLock;
import org.apache.samza.zk.ZkCoordinationUtils;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZkLock {
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private String testZkConnectionString = null;
  private ZkUtils testZkUtils = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;

  ZkCoordinationUtils coordUtils = null;
  Map<String, String> configs;

  @BeforeClass
  public static void setup() throws InterruptedException {
    //zkServer = new EmbeddedZookeeper();
    //zkServer.setup();
  }

  @Before
  public void testSetup() {
    testZkConnectionString = "127.0.0.1:2181";// + zkServer.getPort();

    configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.zk.ZkJobCoordinatorFactory");
    configs.put(ZkConfig.ZK_CONNECT, testZkConnectionString);



    try {
    //  testZkUtils.getZkClient().createPersistent(KEY_BUILDER.getProcessorsPath(), true);
    } catch (ZkNodeExistsException e) {
      // Do nothing
    }
  }

  @Test
  public void testSingleLock() {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.zk.ZkJobCoordinatorFactory");
    configs.put(ZkConfig.ZK_CONNECT, testZkConnectionString);
    coordUtils = new ZkCoordinationUtils("groupId", "p1", new MapConfig(configs));
    ZkCoordinationLock lock = (ZkCoordinationLock) coordUtils.getLock();

    //TestZkUtils.sleepMs(10000);
    try {
      lock.lock(TimeUnit.MILLISECONDS, 100);
    } catch (TimeoutException e) {
      Assert.fail("Timed out");
    }
    System.out.println("DONE");
    lock.unlock();


    try {
      lock.lock(TimeUnit.MILLISECONDS, 100);
    } catch (TimeoutException e) {
      Assert.fail("Timed out1");
    }
    System.out.println("DONE1");
    lock.unlock();

  }

  @Test
  public void testMultipleLock() {
    int numThreads = 50;
    Thread [] tt = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      Runnable runnable = createRunnable("p" + i);
      tt[i] = new Thread(runnable);
      tt[i].start();
    }

    for(Thread t: tt) {
      try {
        t.join();
      } catch (InterruptedException e) {
        System.out.println("t = " + t + " got interrupted.");
      }
    }
  }

  private Runnable createRunnable(String participant) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        coordUtils = new ZkCoordinationUtils("groupId", participant, new MapConfig(configs));
        ZkCoordinationLock lock = (ZkCoordinationLock) coordUtils.getLock();

        try {
          lock.lock(TimeUnit.MILLISECONDS, 10000);
        } catch (TimeoutException e) {
          Assert.fail("Timed out " + participant);
        }
        System.out.println("DONE " + participant);
        lock.unlock();
      }
    };
    return runnable;
  }
}
