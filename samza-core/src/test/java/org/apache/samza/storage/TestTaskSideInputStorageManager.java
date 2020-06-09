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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
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
    final ImmutableMap<SystemStreamPartition, String> processedOffsets = ImmutableMap.of(ssp, offset);

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ImmutableSet.of(ssp))
        .build();
    Map<String, StorageEngine> stores = new HashMap<>();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.flush(processedOffsets);

    for (StorageEngine storageEngine : stores.values()) {
      verify(storageEngine).flush();
    }

    verify(testSideInputStorageManager).writeFileOffsets(eq(processedOffsets));

    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);
    assertTrue("Store directory: " + storeDir.getPath() + " is missing.", storeDir.exists());

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();
    assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
    assertEquals("Mismatch between last processed offset and file offset.", fileOffsets.get(ssp), offset);
  }

  @Test
  public void testStop() {
    final String storeName = "test-stop-store";
    final String taskName = "test-stop-task";
    final SystemStreamPartition ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    final String offset = "123";
    final ImmutableMap<SystemStreamPartition, String> processedOffsets = ImmutableMap.of(ssp, offset);

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, NON_LOGGED_STORE_DIR)
        .addInMemoryStore(storeName, ImmutableSet.of())
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.stop(processedOffsets);

    verify(testSideInputStorageManager.getStore(storeName)).stop();
    verify(testSideInputStorageManager).writeFileOffsets(eq(processedOffsets));
  }

  @Test
  public void testWriteOffsetFilesForNonPersistedStore() {
    final String storeName = "test-write-offset-non-persisted-store";
    final String taskName = "test-write-offset-for-non-persisted-task";

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, NON_LOGGED_STORE_DIR)
        .addInMemoryStore(storeName, ImmutableSet.of())
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.writeFileOffsets(Collections.emptyMap()); // should be no-op
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

    Map<SystemStreamPartition, String> processedOffsets = ImmutableMap.of(ssp, offset, ssp2, offset);

    TaskSideInputStorageManager testSideInputStorageManager = new MockTaskSideInputStorageManagerBuilder(taskName, LOGGED_STORE_DIR)
        .addLoggedStore(storeName, ImmutableSet.of(ssp))
        .addLoggedStore(storeName2, ImmutableSet.of(ssp2))
        .build();

    initializeSideInputStorageManager(testSideInputStorageManager);
    testSideInputStorageManager.writeFileOffsets(processedOffsets);
    File storeDir = testSideInputStorageManager.getStoreLocation(storeName);

    assertTrue("Store directory: " + storeDir.getPath() + " is missing.", storeDir.exists());

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();

    assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
    assertEquals("Mismatch between last processed offset and file offset.", fileOffsets.get(ssp), offset);

    assertTrue("Failed to get offset for ssp: " + ssp2.toString() + " from file.", fileOffsets.containsKey(ssp2));
    assertEquals("Mismatch between last processed offset and file offset.", fileOffsets.get(ssp2), offset);
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
    Map<SystemStreamPartition, String> processedOffsets = ssps.stream()
        .collect(Collectors.toMap(Function.identity(), ssp -> offset));

    testSideInputStorageManager.writeFileOffsets(processedOffsets);

    Map<SystemStreamPartition, String> fileOffsets = testSideInputStorageManager.getFileOffsets();
    ssps.forEach(ssp -> {
        assertTrue("Failed to get offset for ssp: " + ssp.toString() + " from file.", fileOffsets.containsKey(ssp));
        assertEquals("Mismatch between last processed offset and file offset.", fileOffsets.get(ssp), offset);
      });
  }

  private void initializeSideInputStorageManager(TaskSideInputStorageManager testSideInputStorageManager) {
    testSideInputStorageManager.init();
  }

  private static final class MockTaskSideInputStorageManagerBuilder {
    private final TaskName taskName;
    private final String storeBaseDir;

    private Map<String, StorageEngine> stores = new HashMap<>();
    private Map<String, Set<SystemStreamPartition>> storeToSSps = new HashMap<>();
    private Clock clock = mock(Clock.class);

    public MockTaskSideInputStorageManagerBuilder(String taskName, String storeBaseDir) {
      this.taskName = new TaskName(taskName);
      this.storeBaseDir = storeBaseDir;
    }

    MockTaskSideInputStorageManagerBuilder addInMemoryStore(String storeName, Set<SystemStreamPartition> ssps) {
      StorageEngine storageEngine = mock(StorageEngine.class);
      when(storageEngine.getStoreProperties()).thenReturn(
          new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).setPersistedToDisk(false).build());

      stores.put(storeName, storageEngine);
      storeToSSps.put(storeName, ssps);

      return this;
    }

    MockTaskSideInputStorageManagerBuilder addLoggedStore(String storeName, Set<SystemStreamPartition> ssps) {
      StorageEngine storageEngine = mock(StorageEngine.class);
      when(storageEngine.getStoreProperties()).thenReturn(
          new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).setPersistedToDisk(true).build());

      stores.put(storeName, storageEngine);
      storeToSSps.put(storeName, ssps);

      return this;
    }

    TaskSideInputStorageManager build() {
      return spy(new TaskSideInputStorageManager(taskName, TaskMode.Active, new File(storeBaseDir), stores, storeToSSps,
          clock));
    }
  }
}