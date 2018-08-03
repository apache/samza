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

package org.apache.samza.storage;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ScalaJavaUtil;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestTaskSideInputStorageManager {
  private static final String LOGGED_STORE_DIR = System.getProperty("java.io.tmpdir") + File.separator + "logged-store";
  private static final String NON_LOGGED_STORE_DIR = System.getProperty("java.io.tmpdir") + File.separator + "non-logged-store";

  @Test
  public void testInit() {
    final String storeName = "test-init-store";
    final String taskName = "test-init-task";

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ImmutableSet.of())
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);

    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);
    assertTrue("Store directory: " + storeDir.getPath() + " is missing.", storeDir.exists());
  }

  @Test
  public void testFlush() {
    final String storeName = "test-flush-store";
    final String taskName = "test-flush-task";
    final SystemStreamPartition ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    final String offset = "123";

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ImmutableSet.of(ssp))
        .build();
    Map<String, StorageEngine> stores = new HashMap<>();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.updateLastProcessedOffset(ssp, offset);
    testSideInputStorageManager.flush();

    for (StorageEngine storageEngine : stores.values()) {
      verify(storageEngine).flush();
    }

    verify(testSideInputStorageManager).writeOffsetFiles();

    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);
    assertTrue("Store directory: " + storeDir.getPath() + " is missing.", storeDir.exists());

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();
    assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
    assertEquals("Mismatch in between last processed offset and file offset.", fileOffsets.get(ssp), offset);
  }

  @Test
  public void testStop() {
    final String storeName = "test-stop-store";
    final String taskName = "test-stop-task";

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, NON_LOGGED_STORE_DIR)
        .addInMemoryStore(storeName, ImmutableSet.of())
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.stop();

    verify(testSideInputStorageManager.getStore(storeName)).stop();
    verify(testSideInputStorageManager).writeOffsetFiles();
  }

  @Test
  public void testWriteOffsetFilesForNonPersistedStore() {
    final String storeName = "test-write-offset-non-persisted-store";
    final String taskName = "test-write-offset-for-non-persisted-task";

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, NON_LOGGED_STORE_DIR)
        .addInMemoryStore(storeName, ImmutableSet.of())
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.writeOffsetFiles(); // should be no-op
    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);

    assertFalse("Store directory: " + storeDir.getPath() + " should not be created for non-persisted store", storeDir.exists());
  }

  @Test
  public void testWriteOffsetFilesForPersistedStore() {
    final String storeName = "test-write-offset-persisted-store";
    final String storeName2 = "test-write-offset-persisted-store-2";

    final String taskName = "test-write-offset-for-persisted-task";
    final String offset = "123";
    final SystemStreamPartition ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    final SystemStreamPartition ssp2 = new SystemStreamPartition("test-system2", "test-stream2", new Partition(0));

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ImmutableSet.of(ssp))
        .addLoggedStore(storeName2, ImmutableSet.of(ssp2))
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.updateLastProcessedOffset(ssp, offset);
    testSideInputStorageManager.updateLastProcessedOffset(ssp2, offset);
    testSideInputStorageManager.writeOffsetFiles();
    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);

    assertTrue("Store directory: " + storeDir.getPath() + " is missing.", storeDir.exists());

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();

    assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
    assertEquals("Mismatch in between last processed offset and file offset.", fileOffsets.get(ssp), offset);

    assertTrue("Failed to get offset for ssp: " + ssp2.toString() + " from file.", fileOffsets.containsKey(ssp2));
    assertEquals("Mismatch in between last processed offset and file offset.", fileOffsets.get(ssp2), offset);
  }

  @Test
  public void testGetFileOffsets() {
    final String storeName = "test-get-file-offsets-store";
    final String taskName = "test-get-file-offsets-task";
    final String offset = "123";

    Set<SystemStreamPartition> ssps = IntStream.range(1, 6)
        .mapToObj(idx -> new SystemStreamPartition("test-system", "test-stream", new Partition(idx)))
        .collect(Collectors.toSet());

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ssps)
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    ssps.forEach(ssp -> testSideInputStorageManager.updateLastProcessedOffset(ssp, offset));
    testSideInputStorageManager.writeOffsetFiles();

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();

    ssps.forEach(ssp -> {
        assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
        assertEquals("Mismatch in between last processed offset and file offset.", fileOffsets.get(ssp), offset);
      });
  }

  @Test
  public void testGetStartingOffsets() {
    final String storeName = "test-get-starting-offset-store";
    final String taskName = "test-get-starting-offset-task";

    Set<SystemStreamPartition> ssps = IntStream.range(1, 6)
        .mapToObj(idx -> new SystemStreamPartition("test-system", "test-stream", new Partition(idx)))
        .collect(Collectors.toSet());


    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ssps)
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    Map<SystemStreamPartition, String> fileOffsets = ssps.stream()
        .collect(Collectors.toMap(Function.identity(), ssp -> {
            int partitionId = ssp.getPartition().getPartitionId();
            int offset = partitionId % 2 == 0 ? partitionId + 10 : partitionId;
            return String.valueOf(offset);
          }));

    Map<SystemStreamPartition, String> oldestOffsets = ssps.stream()
        .collect(Collectors.toMap(Function.identity(), ssp -> {
            int partitionId = ssp.getPartition().getPartitionId();
            int offset = partitionId % 2 == 0 ? partitionId : partitionId + 10;

            return String.valueOf(offset);
          }));

    doCallRealMethod().when(testSideInputStorageManager).getStartingOffsets(fileOffsets, oldestOffsets);

    Map<SystemStreamPartition, String> startingOffsets =
        testSideInputStorageManager.getStartingOffsets(fileOffsets, oldestOffsets);

    assertTrue("Failed to get starting offsets for all ssps", startingOffsets.size() == 5);
  }

  private void initializeSideInputStorageManager(TaskSideInputStorageManager testSideInputStorageManager) {
    doReturn(new HashMap<>()).when(testSideInputStorageManager).getStartingOffsets(any(), any());
    testSideInputStorageManager.init();
  }

  private static final class MockTaskSideInputStorageManagerBuilder {
    private final TaskName taskName;
    private final String storeBaseDir;

    private Clock clock = mock(Clock.class);
    private Map<String, SideInputsProcessor> storeToProcessor = new HashMap<>();
    private Map<String, StorageEngine> stores = new HashMap<>();
    private Map<String, Set<SystemStreamPartition>> storeToSSps = new HashMap<>();
    private StreamMetadataCache streamMetadataCache = mock(StreamMetadataCache.class);
    private SystemAdmins systemAdmins = mock(SystemAdmins.class);

    public MockTaskSideInputStorageManagerBuilder(String taskName, String storeBaseDir) {
      this.taskName = new TaskName(taskName);
      this.storeBaseDir = storeBaseDir;

      initializeMocks();
    }

    private void initializeMocks() {
      SystemAdmin admin = mock(SystemAdmin.class);
      doAnswer(invocation -> {
          String offset1 = invocation.getArgumentAt(0, String.class);
          String offset2 = invocation.getArgumentAt(1, String.class);

          return Long.compare(Long.parseLong(offset1), Long.parseLong(offset2));
        }).when(admin).offsetComparator(any(), any());
      doAnswer(invocation -> {
          Map<SystemStreamPartition, String> sspToOffsets = invocation.getArgumentAt(0, Map.class);

          return sspToOffsets.entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                  entry -> String.valueOf(Long.parseLong(entry.getValue()) + 1)));
        }).when(admin).getOffsetsAfter(any());
      doReturn(admin).when(systemAdmins).getSystemAdmin("test-system");

      doReturn(ScalaJavaUtil.toScalaMap(new HashMap<>())).when(streamMetadataCache).getStreamMetadata(any(), anyBoolean());
    }

    MockTaskSideInputStorageManagerBuilder addInMemoryStore(String storeName, Set<SystemStreamPartition> ssps) {
      StorageEngine storageEngine = mock(StorageEngine.class);
      when(storageEngine.getStoreProperties()).thenReturn(
          new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).setPersistedToDisk(false).build());

      stores.put(storeName, storageEngine);
      storeToProcessor.put(storeName, mock(SideInputsProcessor.class));
      storeToSSps.put(storeName, ssps);

      return this;
    }

    MockTaskSideInputStorageManagerBuilder addLoggedStore(String storeName, Set<SystemStreamPartition> ssps) {
      StorageEngine storageEngine = mock(StorageEngine.class);
      when(storageEngine.getStoreProperties()).thenReturn(
          new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).setPersistedToDisk(true).build());

      stores.put(storeName, storageEngine);
      storeToProcessor.put(storeName, mock(SideInputsProcessor.class));
      storeToSSps.put(storeName, ssps);

      return this;
    }

    TaskSideInputStorageManager build() {
      return spy(new TaskSideInputStorageManager(taskName, streamMetadataCache, storeBaseDir, stores,
          storeToProcessor, storeToSSps, systemAdmins, mock(Config.class), clock));
    }
  }
}