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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class TestZkUtils {
  private static EmbeddedZookeeper zkServer = null;
  private static final ZkKeyBuilder KEY_BUILDER = new ZkKeyBuilder("test");
  private ZkClient zkClient = null;
  private static final int SESSION_TIMEOUT_MS = 20000;
  private static final int CONNECTION_TIMEOUT_MS = 10000;
  private ZkUtils zkUtils;

  @Rule
  // Declared public to honor junit contract.
  public final ExpectedException expectedException = ExpectedException.none();

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

    zkUtils = getZkUtils();

    zkUtils.connect();
  }

  @After
  public void testTeardown() {
    zkUtils.close();
    zkClient.close();
  }

  private ZkUtils getZkUtils() {
    return new ZkUtils(KEY_BUILDER, zkClient,
                       SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
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
  public void testZKProtocolVersion() {
    // first time connect, version should be set to ZkUtils.ZK_PROTOCOL_VERSION
    ZkLeaderElector le = new ZkLeaderElector("1", zkUtils);
    ZkControllerImpl zkController = new ZkControllerImpl("1", zkUtils, null, le);
    zkController.register();
    String root = zkUtils.getKeyBuilder().getRootPath();
    String ver = (String) zkUtils.getZkClient().readData(root);
    Assert.assertEquals(ZkUtils.ZK_PROTOCOL_VERSION, ver);

    // do it again (in case original value was null
    zkController = new ZkControllerImpl("1", zkUtils, null, le);
    zkController.register();
    ver = (String) zkUtils.getZkClient().readData(root);
    Assert.assertEquals(ZkUtils.ZK_PROTOCOL_VERSION, ver);

    // now negative case
    zkUtils.getZkClient().writeData(root, "2.0");
    try {
      zkController = new ZkControllerImpl("1", zkUtils, null, le);
      zkController.register();
      Assert.fail("Expected to fail because of version mismatch 2.0 vs 1.0");
    } catch (SamzaException e) {
      // expected
    }

    // validate future values, let's say that current version should be 3.0
    try {
      Field f = zkUtils.getClass().getDeclaredField("ZK_PROTOCOL_VERSION");
      FieldUtils.removeFinalModifier(f);
      f.set(null, "3.0");
    } catch (Exception e) {
      System.out.println(e);
      Assert.fail();
    }

    try {
      zkController = new ZkControllerImpl("1", zkUtils, null, le);
      zkController.register();
      Assert.fail("Expected to fail because of version mismatch 2.0 vs 3.0");
    } catch (SamzaException e) {
      // expected
    }
  }

  @Test
  public void testGetProcessorsIDs() {
    Assert.assertEquals(0, zkUtils.getSortedActiveProcessorsIDs().size());
    zkUtils.registerProcessorAndGetId(new ProcessorData("host1", "1"));
    List<String> l = zkUtils.getSortedActiveProcessorsIDs();
    Assert.assertEquals(1, l.size());
    new ZkUtils(KEY_BUILDER, zkClient, SESSION_TIMEOUT_MS, new NoOpMetricsRegistry()).registerProcessorAndGetId(
        new ProcessorData("host2", "2"));
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
    zkUtils.validatePaths(new String[]{root, keyBuilder.getJobModelVersionPath(), keyBuilder.getProcessorsPath()});
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

  /**
   * Create two duplicate processors with same processorId.
   * Second creation should fail with exception.
   */
  @Test
  public void testRegisterProcessorAndGetIdShouldFailForDuplicateProcessorRegistration() {
    final String testHostName = "localhost";
    final String testProcessId = "testProcessorId";
    ProcessorData processorData1 = new ProcessorData(testHostName, testProcessId);
    // Register processor 1 which is not duplicate, this registration should succeed.
    zkUtils.registerProcessorAndGetId(processorData1);

    ZkUtils zkUtils1 = getZkUtils();
    zkUtils1.connect();
    ProcessorData duplicateProcessorData = new ProcessorData(testHostName, testProcessId);
    // Registration of the duplicate processor should fail.
    expectedException.expect(SamzaException.class);
    zkUtils1.registerProcessorAndGetId(duplicateProcessorData);
  }

  @Test
  public void testPublishNewJobModel() {
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder("test");
    String root = keyBuilder.getRootPath();
    zkClient.deleteRecursive(root);
    String version = "1";
    String oldVersion = "0";

    zkUtils.validatePaths(new String[]{root, keyBuilder.getJobModelPathPrefix(), keyBuilder.getJobModelVersionPath()});

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

  @Test
  public void testCleanUpZkJobModels() {
    String root = zkUtils.getKeyBuilder().getJobModelPathPrefix();
    System.out.println("root=" + root);
    zkUtils.getZkClient().createPersistent(root, true);

    // generate multiple version
    for (int i = 101; i < 110; i++) {
      zkUtils.publishJobModel(String.valueOf(i), null);
    }

    // clean all of the versions except 5 most recent ones
    zkUtils.deleteOldJobModels(5);
    Assert.assertEquals(Arrays.asList("105", "106", "107", "108", "109"), zkUtils.getZkClient().getChildren(root));
  }

  @Test
  public void testCleanUpZkBarrierVersion() {
    String root = zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix();
    zkUtils.getZkClient().createPersistent(root, true);
    ZkBarrierForVersionUpgrade barrier = new ZkBarrierForVersionUpgrade(root, zkUtils, null);
    for (int i = 200; i < 210; i++) {
      barrier.create(String.valueOf(i), new ArrayList<>(Arrays.asList(i + "a", i + "b", i + "c")));
    }

    zkUtils.deleteOldBarrierVersions(5);
    List<String> zNodeIds = zkUtils.getZkClient().getChildren(root);
    Collections.sort(zNodeIds);
    Assert.assertEquals(Arrays.asList("barrier_205", "barrier_206", "barrier_207", "barrier_208", "barrier_209"),
        zNodeIds);
  }

  @Test
  public void testCleanUpZk() {
    String pathA = "/path/testA";
    String pathB = "/path/testB";
    zkUtils.getZkClient().createPersistent(pathA, true);
    zkUtils.getZkClient().createPersistent(pathB, true);

    // Create 100 nodes
    for (int i = 0; i < 20; i++) {
      String p1 = pathA + "/" + i;
      zkUtils.getZkClient().createPersistent(p1, true);
      zkUtils.getZkClient().createPersistent(p1 + "/something1", true);
      zkUtils.getZkClient().createPersistent(p1 + "/something2", true);

      String p2 = pathB + "/some_" + i;
      zkUtils.getZkClient().createPersistent(p2, true);
      zkUtils.getZkClient().createPersistent(p2 + "/something1", true);
      zkUtils.getZkClient().createPersistent(p2 + "/something2", true);
    }

    List<String> zNodeIds = new ArrayList<>();
    // empty list
    zkUtils.deleteOldVersionPath(pathA, zNodeIds, 10, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });


    zNodeIds = zkUtils.getZkClient().getChildren(pathA);
    zkUtils.deleteOldVersionPath(pathA, zNodeIds, 10, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return Integer.valueOf(o1) - Integer.valueOf(o2);
      }
    });

    for (int i = 0; i < 10; i++) {
      // should be gone
      String p1 = pathA + "/" + i;
      Assert.assertFalse("path " + p1 + " exists", zkUtils.getZkClient().exists(p1));
    }

    for (int i = 10; i < 20; i++) {
      // should be gone
      String p1 = pathA + "/" + i;
      Assert.assertTrue("path " + p1 + " exists", zkUtils.getZkClient().exists(p1));
    }

    zNodeIds = zkUtils.getZkClient().getChildren(pathB);
    zkUtils.deleteOldVersionPath(pathB, zNodeIds, 1, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return Integer.valueOf(o1.substring(o1.lastIndexOf("_") + 1)) - Integer
            .valueOf(o2.substring(o2.lastIndexOf("_") + 1));
      }
    });

    for (int i = 0; i < 19; i++) {
      // should be gone
      String p1 = pathB + "/" + i;
      Assert.assertFalse("path " + p1 + " exists", zkUtils.getZkClient().exists(p1));
    }

    for (int i = 19; i < 20; i++) {
      // should be gone
      String p1 = pathB + "/some_" + i;
      Assert.assertTrue("path " + p1 + " exists", zkUtils.getZkClient().exists(p1));
    }

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
  @Test
  public void testgetNextJobModelVersion() {
    // Set up the Zk base paths for testing.
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder("test");
    String root = keyBuilder.getRootPath();
    zkClient.deleteRecursive(root);
    zkUtils.validatePaths(new String[]{root, keyBuilder.getJobModelPathPrefix(), keyBuilder.getJobModelVersionPath()});

    String version = "1";
    String oldVersion = "0";

    // Set zkNode JobModelVersion to 1.
    zkUtils.publishJobModelVersion(oldVersion, version);

    Assert.assertEquals(version, zkUtils.getJobModelVersion());

    // Publish JobModel with a higher version (2).
    zkUtils.publishJobModel("2", new JobModel(new MapConfig(), new HashMap<>()));

    // Get on the JobModel version should return 2, taking into account the published version 2.
    Assert.assertEquals("3", zkUtils.getNextJobModelVersion(zkUtils.getJobModelVersion()));
  }
}
