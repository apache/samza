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

package org.apache.samza.checkpoint.azure;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.AzureConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

@Ignore("Requires Azure account credentials")
public class ITestAzureCheckpointManager {

  private static String storageConnectionString = "";
  private static CheckpointManager checkpointManager;

  @BeforeClass
  public static void setupAzureTable() {
    AzureCheckpointManagerFactory factory = new AzureCheckpointManagerFactory();
    checkpointManager = factory.getCheckpointManager(getConfig(), new NoOpMetricsRegistry());

    checkpointManager.start();
    checkpointManager.clearCheckpoints();
  }

  @AfterClass
  public static void teardownAzureTable() {
    checkpointManager.clearCheckpoints();
    checkpointManager.stop();
  }

  private static Config getConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(AzureConfig.AZURE_STORAGE_CONNECT, storageConnectionString);

    return new MapConfig(configMap);
  }

  @Test
  public void testStoringAndReadingCheckpointsSamePartition() {
    Partition partition = new Partition(0);
    TaskName taskName = new TaskName("taskName0");
    SystemStreamPartition ssp = new SystemStreamPartition("Azure", "Stream", partition);
    Map<SystemStreamPartition, String> sspMap = new HashMap<>();

    sspMap.put(ssp, "12345");
    Checkpoint cp0 = new Checkpoint(sspMap);

    sspMap.put(ssp, "54321");
    Checkpoint cp1 = new Checkpoint(sspMap);

    checkpointManager.register(taskName);

    checkpointManager.writeCheckpoint(taskName, cp0);
    Checkpoint readCp = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp0, readCp);

    checkpointManager.writeCheckpoint(taskName, cp1);
    Checkpoint readCp1 = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp1, readCp1);
  }

  @Test
  public void testStoringAndReadingCheckpointsMultiPartitions() {
    Partition partition = new Partition(0);
    Partition partition1 = new Partition(1);
    TaskName taskName = new TaskName("taskName");
    SystemStreamPartition ssp = new SystemStreamPartition("Azure", "Stream", partition);
    SystemStreamPartition ssp1 = new SystemStreamPartition("Azure", "Stream", partition1);

    Map<SystemStreamPartition, String> sspMap = new HashMap<>();
    sspMap.put(ssp, "12345");
    sspMap.put(ssp1, "54321");
    Checkpoint cp1 = new Checkpoint(sspMap);

    Map<SystemStreamPartition, String> sspMap2 = new HashMap<>();
    sspMap2.put(ssp, "12347");
    sspMap2.put(ssp1, "54323");
    Checkpoint cp2 = new Checkpoint(sspMap2);

    checkpointManager.register(taskName);

    checkpointManager.writeCheckpoint(taskName, cp1);
    Checkpoint readCp1 = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp1, readCp1);

    checkpointManager.writeCheckpoint(taskName, cp2);
    Checkpoint readCp2 = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp2, readCp2);
  }

  @Test
  public void testStoringAndReadingCheckpointsMultiTasks() {
    Partition partition = new Partition(0);
    Partition partition1 = new Partition(1);
    TaskName taskName = new TaskName("taskName1");
    TaskName taskName1 = new TaskName("taskName2");
    SystemStreamPartition ssp = new SystemStreamPartition("Azure", "Stream", partition);
    SystemStreamPartition ssp1 = new SystemStreamPartition("Azure", "Stream", partition1);

    Map<SystemStreamPartition, String> sspMap = new HashMap<>();
    sspMap.put(ssp, "12345");
    sspMap.put(ssp1, "54321");
    Checkpoint cp1 = new Checkpoint(sspMap);

    Map<SystemStreamPartition, String> sspMap2 = new HashMap<>();
    sspMap2.put(ssp, "12347");
    sspMap2.put(ssp1, "54323");
    Checkpoint cp2 = new Checkpoint(sspMap2);

    checkpointManager.register(taskName);
    checkpointManager.register(taskName1);

    checkpointManager.writeCheckpoint(taskName, cp1);
    checkpointManager.writeCheckpoint(taskName1, cp2);

    Checkpoint readCp1 = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertNotNull(readCp1);
    Assert.assertEquals(cp1, readCp1);

    Checkpoint readCp2 = checkpointManager.readLastCheckpoint(taskName1);
    Assert.assertNotNull(readCp2);
    Assert.assertEquals(cp2, readCp2);

    checkpointManager.writeCheckpoint(taskName, cp2);
    checkpointManager.writeCheckpoint(taskName1, cp1);

    readCp1 = checkpointManager.readLastCheckpoint(taskName1);
    Assert.assertEquals(cp1, readCp1);

    readCp2 = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp2, readCp2);
  }

  @Test
  public void testMultipleBatchWrites() {
    TaskName taskName = new TaskName("taskName3");
    Map<SystemStreamPartition, String> sspMap = new HashMap<>();

    final int testBatchNum = 2;
    final int testOffsetNum = testBatchNum * AzureCheckpointManager.MAX_WRITE_BATCH_SIZE;

    for (int i = 0; i < testOffsetNum; i++) {
      Partition partition = new Partition(i);
      SystemStreamPartition ssp = new SystemStreamPartition("Azure", "Stream", partition);
      sspMap.put(ssp, String.valueOf(i));
    }

    Checkpoint cp0 = new Checkpoint(sspMap);
    checkpointManager.register(taskName);
    checkpointManager.writeCheckpoint(taskName, cp0);
    Checkpoint readCp = checkpointManager.readLastCheckpoint(taskName);
    Assert.assertEquals(cp0, readCp);
  }
}
