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

import java.util.List;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class TestZkUtils {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private ZkClient zkClient = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;
  private ZkUtils zkUtils;

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

    zkUtils = new ZkUtils(
        KEY_BUILDER,
        zkClient,
        SESSION_TIMEOUT_MS);

    zkUtils.connect();
  }

  @After
  public void testTeardown() {
    zkUtils.close();
    zkClient.close();
  }

  @AfterClass
  public static void teardown() {
    zkServer.teardown();
  }

  @Test
  public void testRegisterProcessorId() {
    String assignedPath = zkUtils.registerProcessorAndGetId(new ProcessorData("host", "1"));
    Assert.assertTrue(assignedPath.startsWith(KEY_BUILDER.getProcessorsPath()));

    // Calling registerProcessorId again should return the same ephemeralPath as long as the session is valid
    Assert.assertTrue(zkUtils.registerProcessorAndGetId(new ProcessorData("host", "1")).equals(assignedPath));

  }

  @Test
  public void testGetActiveProcessors() {
    Assert.assertEquals(0, zkUtils.getSortedActiveProcessorsZnodes().size());
    zkUtils.registerProcessorAndGetId(new ProcessorData("processorData", "1"));
    Assert.assertEquals(1, zkUtils.getSortedActiveProcessorsZnodes().size());
  }

  @Test
  public void testGetProcessorsIDs() {
    Assert.assertEquals(0, zkUtils.getSortedActiveProcessorsIDs().size());
    zkUtils.registerProcessorAndGetId(new ProcessorData("host1", "1"));
    List<String> l = zkUtils.getSortedActiveProcessorsIDs();
    Assert.assertEquals(1, l.size());
    new ZkUtils(KEY_BUILDER, zkClient, SESSION_TIMEOUT_MS).registerProcessorAndGetId(new ProcessorData("host2", "2"));
    l = zkUtils.getSortedActiveProcessorsIDs();
    Assert.assertEquals(2, l.size());

    Assert.assertEquals(" ID1 didn't match", "1", l.get(0));
    Assert.assertEquals(" ID2 didn't match", "2", l.get(1));
  }
  
  @Test
  public void testSubscribeToJobModelVersionChange() {

    ZkKeyBuilder keyBuilder = new ZkKeyBuilder("test");
    String root = keyBuilder.getRootPath();
    zkClient.deleteRecursive(root);

    class Result {
      String res = "";
      public String getRes() {
        return res;
      }
      public void updateRes(String newRes) {
        res = newRes;
      }
    }

    Assert.assertFalse(zkUtils.exists(root));

    // create the paths
    zkUtils.makeSurePersistentPathsExists(
        new String[]{root, keyBuilder.getJobModelVersionPath(), keyBuilder.getProcessorsPath()});
    Assert.assertTrue(zkUtils.exists(root));
    Assert.assertTrue(zkUtils.exists(keyBuilder.getJobModelVersionPath()));
    Assert.assertTrue(zkUtils.exists(keyBuilder.getProcessorsPath()));

    final Result res = new Result();
    // define the callback
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
        res.updateRes((String) data);
      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        Assert.fail("Data wasn't deleted;");
      }
    };
    // subscribe
    zkClient.subscribeDataChanges(keyBuilder.getJobModelVersionPath(), dataListener);
    zkClient.subscribeDataChanges(keyBuilder.getProcessorsPath(), dataListener);
    // update
    zkClient.writeData(keyBuilder.getJobModelVersionPath(), "newVersion");

    // verify
    Assert.assertTrue(testWithDelayBackOff(() -> "newVersion".equals(res.getRes()), 2, 1000));

    // update again
    zkClient.writeData(keyBuilder.getProcessorsPath(), "newProcessor");

    Assert.assertTrue(testWithDelayBackOff(() -> "newProcessor".equals(res.getRes()), 2, 1000));
  }

  @Test
  public void testPublishNewJobModel() {
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder("test");
    String root = keyBuilder.getRootPath();
    zkClient.deleteRecursive(root);
    String version = "1";
    String oldVersion = "0";

    zkUtils.makeSurePersistentPathsExists(
        new String[]{root, keyBuilder.getJobModelPathPrefix(), keyBuilder.getJobModelVersionPath()});

    zkUtils.publishJobModelVersion(oldVersion, version);
    Assert.assertEquals(version, zkUtils.getJobModelVersion());

    String newerVersion = Long.toString(Long.valueOf(version) + 1);
    zkUtils.publishJobModelVersion(version, newerVersion);
    Assert.assertEquals(newerVersion, zkUtils.getJobModelVersion());

    try {
      zkUtils.publishJobModelVersion(oldVersion, "10"); //invalid new version
      Assert.fail("publish invalid version should've failed");
    } catch (SamzaException e) {
      // expected
    }

    // create job model
    Map<String, String> configMap = new HashMap<>();
    Map<String, ContainerModel> containers = new HashMap<>();
    MapConfig config = new MapConfig(configMap);
    JobModel jobModel = new JobModel(config, containers);

    zkUtils.publishJobModel(version, jobModel);
    Assert.assertEquals(jobModel, zkUtils.getJobModel(version));
  }

  public static boolean testWithDelayBackOff(BooleanSupplier cond, long startDelayMs, long maxDelayMs) {
    long delay = startDelayMs;
    while (delay < maxDelayMs) {
      if (cond.getAsBoolean())
        return true;
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        return false;
      }
      delay *= 2;
    }
    return false;
  }

  public static void sleepMs(long delay) {
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Assert.fail("Sleep was interrupted");
    }
  }
}
