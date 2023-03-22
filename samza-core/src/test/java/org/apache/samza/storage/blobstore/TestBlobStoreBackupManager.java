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

package org.apache.samza.storage.blobstore;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV1;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.metrics.BlobStoreBackupManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreTestUtil;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.storage.blobstore.util.DirDiffUtil;
import org.apache.samza.util.Clock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;


public class TestBlobStoreBackupManager {
  private final ExecutorService mockExecutor = MoreExecutors.newDirectExecutorService();
  // mock container - task - job models
  private final JobModel jobModel = mock(JobModel.class);
  private final ContainerModel containerModel = mock(ContainerModel.class);
  private final TaskModel taskModel = mock(TaskModel.class, RETURNS_DEEP_STUBS);
  private final Clock clock = mock(Clock.class);
  private final BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);
  private final BlobStoreManager blobStoreManager = mock(BlobStoreManager.class);
  private final StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);

  //job and store definition
  private final CheckpointId checkpointId = CheckpointId.deserialize("1234-567");
  private final String jobName = "testJobName";
  private final String jobId = "testJobID";
  private final String taskName = "testTaskName";
  private final String prevSnapshotIndexBlobId = "testPrevBlobId";
  private Map<String, StorageEngine> storeStorageEngineMap = new HashMap<>();
  private Map<String, String> mapConfig = new HashMap<>();

  private final MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
  private final Counter counter = mock(Counter.class);
  private final Timer timer = mock(Timer.class);
  private final Gauge<Long> longGauge = mock(Gauge.class);
  private final Gauge<AtomicLong> atomicLongGauge = mock(Gauge.class);

  private BlobStoreBackupManager blobStoreBackupManager;
  private BlobStoreBackupManagerMetrics blobStoreTaskBackupMetrics;

  // Remote and local snapshot definitions
  private Map<String, SnapshotIndex> testBlobStore = new HashMap<>();
  private Map<String, Pair<String, SnapshotIndex>> indexBlobIdAndLocalRemoteSnapshotsPair;
  private Map<String, String> testStoreNameAndSCMMap;

  @Before
  public void setup() throws Exception {
    when(clock.currentTimeMillis()).thenReturn(1234567L);
    // setup test local and remote snapshots
    indexBlobIdAndLocalRemoteSnapshotsPair = setupRemoteAndLocalSnapshots(true);
    // setup test store name and SCMs map
    testStoreNameAndSCMMap = setupTestStoreSCMMapAndStoreBackedFactoryConfig(indexBlobIdAndLocalRemoteSnapshotsPair);
    // setup: setup task backup manager with expected storeName->storageEngine map
    testStoreNameAndSCMMap.forEach((storeName, scm) -> storeStorageEngineMap.put(storeName, null));

    mapConfig.putAll(new MapConfig(ImmutableMap.of("job.name", jobName, "job.id", jobId)));

    Config config = new MapConfig(mapConfig);

    // Mock - return snapshot index for blob id from test blob store map
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(blobStoreUtil.getSnapshotIndex(captor.capture(), any(Metadata.class)))
        .then((Answer<CompletableFuture<SnapshotIndex>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          return CompletableFuture.completedFuture(testBlobStore.get(blobId));
        });

//    doNothing().when(blobStoreManager).init();
    when(taskModel.getTaskName().getTaskName()).thenReturn(taskName);
    when(taskModel.getTaskMode()).thenReturn(TaskMode.Active);

    when(metricsRegistry.newCounter(anyString(), anyString())).thenReturn(counter);
    when(metricsRegistry.newGauge(anyString(), anyString(), anyLong())).thenReturn(longGauge);
    when(metricsRegistry.newGauge(anyString(), anyString(), any(AtomicLong.class))).thenReturn(atomicLongGauge);
    when(atomicLongGauge.getValue()).thenReturn(new AtomicLong());
    when(metricsRegistry.newTimer(anyString(), anyString())).thenReturn(timer);
    blobStoreTaskBackupMetrics = new BlobStoreBackupManagerMetrics(metricsRegistry);

    blobStoreBackupManager =
        new MockBlobStoreBackupManager(jobModel, containerModel, taskModel, mockExecutor,
            blobStoreTaskBackupMetrics, config,
            Files.createTempDirectory("logged-store-").toFile(), storageManagerUtil, blobStoreManager);
  }

  @Test
  public void testInitWithInvalidCheckpoint() {
    // init called with null checkpoint storeStorageEngineMap
    blobStoreBackupManager.init(null);
    // verify delete snapshot index blob called from init 0 times because prevSnapshotMap returned from init is empty
    // in case of null checkpoint.
    verify(blobStoreUtil, times(0)).deleteSnapshotIndexBlob(anyString(), any(Metadata.class));
    when(blobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(), any(Checkpoint.class), anySetOf(String.class))).thenCallRealMethod();

    // init called with Checkpoint V1 -> unsupported
    Checkpoint checkpoint = new CheckpointV1(new HashMap<>());
    try {
      blobStoreBackupManager.init(checkpoint);
    } catch (SamzaException exception) {
      Assert.fail("Checkpoint V1 is expected to only log warning.");
    }
  }

  @Test
  public void testUploadWithNoPreviousCheckpoints() throws IOException {
    // Track directory for post cleanup
    List<String> checkpointDirsToClean = new ArrayList<>();

    // Setup: init local/remote snapshots and back manager with no previous checkpoints
    indexBlobIdAndLocalRemoteSnapshotsPair = setupRemoteAndLocalSnapshots(false);
    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, new HashMap<>(),
            ImmutableMap.of(BlobStoreStateBackendFactory.class.getName(), new HashMap<>()));
    blobStoreBackupManager.init(checkpoint);

    // mock: set task store dir to return corresponding test local store and create checkpoint dir
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(storageManagerUtil.getTaskStoreDir(any(File.class), stringCaptor.capture(), any(TaskName.class), any(TaskMode.class)))
        .then((Answer<File>) invocation -> {
          String storeName = invocation.getArgumentAt(1, String.class);
          String snapshotIndexBlobId = testStoreNameAndSCMMap.get(storeName);
          String storeDir = indexBlobIdAndLocalRemoteSnapshotsPair.get(snapshotIndexBlobId).getLeft();
          try {
            BlobStoreTestUtil.createTestCheckpointDirectory(storeDir, checkpointId.serialize()); // create test checkpoint dir
            checkpointDirsToClean.add(storeDir + "-" + checkpointId.serialize()); // track checkpoint dir to cleanup later
          } catch (IOException e) {
            Assert.fail("Couldn't create checkpoint directory. Test failed.");
          }
          return new File(storeDir);
        });


    ArgumentCaptor<File> storeDirCaptor = ArgumentCaptor.forClass(File.class);
    when(storageManagerUtil.getStoreCheckpointDir(storeDirCaptor.capture(), eq(checkpointId)))
        .thenAnswer(new Answer<String>() {
          @Override
          public String answer(InvocationOnMock invocation) throws Throwable {
            File storeDir = invocation.getArgumentAt(0, File.class);
            return storeDir.getAbsolutePath() + "-" + checkpointId.serialize();
          }
        });

    SortedSet<DirDiff> actualDirDiffs = new TreeSet<>(Comparator.comparing(DirDiff::getDirName));
    // mock: mock putDir and capture DirDiff
    ArgumentCaptor<DirDiff> dirDiffCaptor = ArgumentCaptor.forClass(DirDiff.class);
    ArgumentCaptor<SnapshotMetadata> snapshotMetadataCaptor = ArgumentCaptor.forClass(SnapshotMetadata.class);
    when(blobStoreUtil.putDir(dirDiffCaptor.capture(), snapshotMetadataCaptor.capture()))
        .then((Answer<CompletableFuture<DirIndex>>) invocation -> {
          DirDiff dirDiff = invocation.getArgumentAt(0, DirDiff.class);
          SnapshotMetadata snapshotMetadata = invocation.getArgumentAt(1, SnapshotMetadata.class);
          actualDirDiffs.add(dirDiff);
          SnapshotIndex snapshotIndex = testBlobStore.get(testStoreNameAndSCMMap.get(snapshotMetadata.getStoreName()));
          return CompletableFuture.completedFuture(snapshotIndex.getDirIndex());
        });

    SortedSet<SnapshotIndex> expectedSnapshotIndexesUploaded = indexBlobIdAndLocalRemoteSnapshotsPair.values().stream()
        .map(Pair::getRight)
        .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(SnapshotIndex::getCreationTimeMillis))));
    String expectedPreviousSnapshotIndexBlobId = "empty";
    // mock: mock putSnapshotIndex and capture previous snapshot index
    SortedSet<SnapshotIndex> actualSnapshotIndexesUploaded =
        new TreeSet<>(Comparator.comparing(SnapshotIndex::getCreationTimeMillis));
    final String[] actualPreviousSnapshotIndexBlobId = {"empty"};
    ArgumentCaptor<SnapshotIndex> snapshotIndexCaptor = ArgumentCaptor.forClass(SnapshotIndex.class);
    when(blobStoreUtil.putSnapshotIndex(snapshotIndexCaptor.capture()))
        .then((Answer<CompletableFuture<String>>) invocation -> {
          SnapshotIndex snapshotIndex = invocation.getArgumentAt(0, SnapshotIndex.class);
          actualSnapshotIndexesUploaded.add(snapshotIndex);
          if (!snapshotIndex.getPrevSnapshotIndexBlobId().equals(Optional.empty())) {
            actualPreviousSnapshotIndexBlobId[0] = "not-empty";
          }
          return CompletableFuture.completedFuture("random-blob-id");
        });

    // execute
    blobStoreBackupManager.upload(checkpointId, testStoreNameAndSCMMap);

    // setup expected dir diffs after execute: needs checkpoint dirs created in upload()
    TreeSet<DirDiff> expectedDirDiffs = indexBlobIdAndLocalRemoteSnapshotsPair.values().stream()
        .map(localRemoteSnapshotPair -> {
          File localCheckpointDir = new File(localRemoteSnapshotPair.getLeft() + "-" + checkpointId.serialize());
          DirIndex dirIndex = new DirIndex(localCheckpointDir.getName(), Collections.emptyList(), Collections.emptyList(),
              Collections.emptyList(), Collections.emptyList());
          return DirDiffUtil.getDirDiff(localCheckpointDir, dirIndex, DirDiffUtil.areSameFile(false));
        }).collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DirDiff::getDirName))));

    // assert - asset all DirDiff are put to blob store
    Assert.assertEquals(actualDirDiffs, expectedDirDiffs);
    // assert - assert no previous snapshot indexes were found
    Assert.assertEquals(actualPreviousSnapshotIndexBlobId[0], expectedPreviousSnapshotIndexBlobId);
    // assert - assert all snapshot indexes are uploaded
    Assert.assertEquals(actualSnapshotIndexesUploaded, expectedSnapshotIndexesUploaded);

    // cleanup
    checkpointDirsToClean.forEach(path -> {
      try {
        if (Files.exists(Paths.get(path)) && Files.isDirectory(Paths.get(path))) {
          PathUtils.deleteDirectory(Paths.get(path));
        }
      } catch (IOException exception) {
        Assert.fail("Failed to cleanup temporary checkpoint dirs.");
      }
    });
  }

  @Test
  public void testUploadWithPreviousCheckpoints() throws IOException {
    // Track directory for post cleanup
    List<String> checkpointDirsToClean = new ArrayList<>();

    // Setup: init back manager with previous checkpoints
    //indexBlobIdAndLocalRemoteSnapshotsPair = setupRemoteAndLocalSnapshots(true);
    Map<String, String> previousCheckpoints =
        // map store name, previous snapshot index blob id
        indexBlobIdAndLocalRemoteSnapshotsPair.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getValue().getLeft(),
              e -> e.getValue().getRight().getPrevSnapshotIndexBlobId().get()));

    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, new HashMap<>(),
            ImmutableMap.of(BlobStoreStateBackendFactory.class.getName(), previousCheckpoints));
    when(blobStoreUtil.getStoreSnapshotIndexes(anyString(), anyString(), anyString(), any(Checkpoint.class), anySetOf(String.class))).thenCallRealMethod();
    blobStoreBackupManager.init(checkpoint);

    // mock: set task store dir to return corresponding test local store and create checkpoint dir
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(storageManagerUtil.getTaskStoreDir(any(File.class), stringCaptor.capture(), any(TaskName.class), any(TaskMode.class)))
        .then((Answer<File>) invocation -> {
          String storeName = invocation.getArgumentAt(1, String.class);
          String snapshotIndexBlobId = testStoreNameAndSCMMap.get(storeName);
          String storeDir = indexBlobIdAndLocalRemoteSnapshotsPair.get(snapshotIndexBlobId).getLeft();
          try { // create test checkpoint dir
            BlobStoreTestUtil.createTestCheckpointDirectory(storeDir, checkpointId.serialize());
            checkpointDirsToClean.add(storeDir + "-" + checkpointId.serialize());
          } catch (IOException e) {
            Assert.fail("Couldn't create checkpoint directory. Test failed.");
          }
          return new File(storeDir);
        });

    ArgumentCaptor<File> storeDirCaptor = ArgumentCaptor.forClass(File.class);
    when(storageManagerUtil.getStoreCheckpointDir(storeDirCaptor.capture(), eq(checkpointId)))
        .thenAnswer(new Answer<String>() {
          @Override
          public String answer(InvocationOnMock invocation) throws Throwable {
            File storeDir = invocation.getArgumentAt(0, File.class);
            return storeDir.getAbsolutePath() + "-" + checkpointId.serialize();
          }
        });

    // mock: mock putDir and capture DirDiff
    SortedSet<DirDiff> actualDirDiffs = new TreeSet<>(Comparator.comparing(DirDiff::getDirName));
    ArgumentCaptor<DirDiff> dirDiffCaptor = ArgumentCaptor.forClass(DirDiff.class);
    ArgumentCaptor<SnapshotMetadata> snapshotMetadataCaptor = ArgumentCaptor.forClass(SnapshotMetadata.class);
    when(blobStoreUtil.putDir(dirDiffCaptor.capture(), snapshotMetadataCaptor.capture()))
        .then((Answer<CompletableFuture<DirIndex>>) invocation -> {
          DirDiff dirDiff = invocation.getArgumentAt(0, DirDiff.class);
          SnapshotMetadata snapshotMetadata = invocation.getArgumentAt(1, SnapshotMetadata.class);
          actualDirDiffs.add(dirDiff);
          SnapshotIndex snapshotIndex = testBlobStore.get(testStoreNameAndSCMMap.get(snapshotMetadata.getStoreName()));
          return CompletableFuture.completedFuture(snapshotIndex.getDirIndex());
        });

    // mock: mock putSnapshotIndex and capture previous snapshot index
    SortedSet<SnapshotIndex> expectedSnapshotIndexesUploaded = indexBlobIdAndLocalRemoteSnapshotsPair.values().stream()
        .map(Pair::getRight)
        .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(SnapshotIndex::getCreationTimeMillis))));
    SortedSet<SnapshotIndex> actualSnapshotIndexesUploaded = new TreeSet<>(Comparator.comparing(SnapshotIndex::getCreationTimeMillis));
    SortedSet<String> actualPreviousSnapshotIndexBlobIds = new TreeSet<>();
    SortedSet<String> expectedPreviousSnapshotIndexBlobIds = new TreeSet<>(previousCheckpoints.values());
    ArgumentCaptor<SnapshotIndex> snapshotIndexCaptor = ArgumentCaptor.forClass(SnapshotIndex.class);
    when(blobStoreUtil.putSnapshotIndex(snapshotIndexCaptor.capture()))
        .then((Answer<CompletableFuture<String>>) invocation -> {
          SnapshotIndex snapshotIndex = invocation.getArgumentAt(0, SnapshotIndex.class);
          actualSnapshotIndexesUploaded.add(snapshotIndex);
          if (snapshotIndex.getPrevSnapshotIndexBlobId().isPresent()) {
            actualPreviousSnapshotIndexBlobIds.add(snapshotIndex.getPrevSnapshotIndexBlobId().get());
          }
          return CompletableFuture.completedFuture("random-blob-id");
        });

    // execute
    blobStoreBackupManager.upload(checkpointId, ImmutableMap.of());

    TreeSet<DirDiff> expectedDirDiffs = indexBlobIdAndLocalRemoteSnapshotsPair.values()
        .stream()
        .map(localRemoteSnapshotPair ->
            DirDiffUtil.getDirDiff(new File(localRemoteSnapshotPair.getLeft() + "-" + checkpointId.serialize()),
            localRemoteSnapshotPair.getRight().getDirIndex(), DirDiffUtil.areSameFile(false)))
        .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DirDiff::getDirName))));

    // assert - asset all DirDiff are put to blob store
    Assert.assertEquals(actualDirDiffs, expectedDirDiffs);
    // assert - assert no previous snapshot indexes were found
    Assert.assertEquals(actualPreviousSnapshotIndexBlobIds, expectedPreviousSnapshotIndexBlobIds);
    // assert - assert all snapshot indexes are uploaded
    Assert.assertEquals(actualSnapshotIndexesUploaded, expectedSnapshotIndexesUploaded);

    // cleanup
    checkpointDirsToClean.forEach(path -> {
      try {
        if (Files.exists(Paths.get(path)) && Files.isDirectory(Paths.get(path))) {
          PathUtils.deleteDirectory(Paths.get(path));
        }
      } catch (IOException exception) {
        Assert.fail("Failed to cleanup temporary checkpoint dirs.");
      }
    });
  }



  @Test
  public void testCleanupRemovesTTLForAllIndexBlobs() {
    SortedSet<String> actualRemoveTTLsResult = new TreeSet<>(testStoreNameAndSCMMap.values());

    SortedSet<String> expectedRemoveTTLsResult = new TreeSet<>();

    // mock
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(blobStoreUtil.removeTTL(captor.capture(), any(SnapshotIndex.class), any(Metadata.class)))
        .then((Answer<CompletionStage<Void>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          expectedRemoveTTLsResult.add(blobId);
          return CompletableFuture.completedFuture(null);
        });

    // stub out non-tested methods
    when(blobStoreUtil.cleanUpDir(any(DirIndex.class), any(Metadata.class))).thenReturn(CompletableFuture.completedFuture(null));
    when(blobStoreUtil.deleteSnapshotIndexBlob(any(String.class), any(Metadata.class))).thenReturn(CompletableFuture.completedFuture(null));

    // execute
    blobStoreBackupManager.cleanUp(checkpointId, testStoreNameAndSCMMap);

    // Assert
    Assert.assertEquals(actualRemoveTTLsResult, expectedRemoveTTLsResult);
  }

  @Test
  public void testCleanupCleansUpRemoteSnapshot() throws Exception {
    SortedSet<DirIndex> actualCleanedupDirs = indexBlobIdAndLocalRemoteSnapshotsPair.values().stream()
        .map(remoteLocalPair -> remoteLocalPair.getRight().getDirIndex())
        .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DirIndex::getDirName))));

    SortedSet<DirIndex> expectedCleanupDirs = new TreeSet<>(Comparator.comparing(DirIndex::getDirName));

    // mock
    ArgumentCaptor<DirIndex> captor = ArgumentCaptor.forClass(DirIndex.class);
    when(blobStoreUtil.cleanUpDir(captor.capture(), any(Metadata.class)))
        .then((Answer<CompletionStage<Void>>) invocation -> {
          DirIndex dirIndex = invocation.getArgumentAt(0, DirIndex.class);
          expectedCleanupDirs.add(dirIndex);
          return CompletableFuture.completedFuture(null);
        });

    // stub out non-tested methods
    when(blobStoreUtil.removeTTL(anyString(), any(SnapshotIndex.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(blobStoreUtil.deleteSnapshotIndexBlob(any(String.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    // execute
    blobStoreBackupManager.cleanUp(checkpointId, testStoreNameAndSCMMap);

    // Assert
    Assert.assertEquals(actualCleanedupDirs, expectedCleanupDirs);
  }

  @Test
  public void testCleanupRemovesOldSnapshots() throws Exception {
    TreeSet<String> expectedOldSnapshotsRemoved = indexBlobIdAndLocalRemoteSnapshotsPair.values().stream()
        .map(remoteLocalPair -> {
          Optional<String> prevSnapshotIndexBlobId = remoteLocalPair.getRight().getPrevSnapshotIndexBlobId();
          return prevSnapshotIndexBlobId.orElse(null);
        })
        .collect(Collectors.toCollection(TreeSet::new));

    SortedSet<String> actualOldSnapshotsRemoved = new TreeSet<>();

    // mock
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(blobStoreUtil.deleteSnapshotIndexBlob(captor.capture(), any(Metadata.class)))
        .then((Answer<CompletionStage<Void>>) invocation -> {
          String prevIndexBlobId = invocation.getArgumentAt(0, String.class);
          actualOldSnapshotsRemoved.add(prevIndexBlobId);
          return CompletableFuture.completedFuture(null);
        });

    // stub out non-tested methods
    when(blobStoreUtil.removeTTL(anyString(), any(SnapshotIndex.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(blobStoreUtil.cleanUpDir(any(DirIndex.class), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    // execute
    blobStoreBackupManager.cleanUp(checkpointId, testStoreNameAndSCMMap);

    // Assert
    Assert.assertEquals(actualOldSnapshotsRemoved, expectedOldSnapshotsRemoved);
  }

  @Test
  public void testCleanupIgnoresStoresNotConfiguredWithBlobStoreStateBackend() throws Exception {
    // TODO HIGH shesharm Complete test
  }

  private Map<String, String> setupTestStoreSCMMapAndStoreBackedFactoryConfig(Map<String,
      Pair<String, SnapshotIndex>> indexBlobIdAndRemoteAndLocalSnapshotMap) {
    Map<String, String> storeNameSCMMap = new HashMap<>();
    indexBlobIdAndRemoteAndLocalSnapshotMap
        .forEach((blobId, localRemoteSnapshots) -> {
          mapConfig.put("stores." + localRemoteSnapshots.getLeft() + ".factory",
              BlobStoreStateBackendFactory.class.getName());
          mapConfig.put("stores." + localRemoteSnapshots.getLeft() + ".backup.factories",
              BlobStoreStateBackendFactory.class.getName());
          storeNameSCMMap.put(localRemoteSnapshots.getLeft(), blobId);
        });
    return storeNameSCMMap;
  }

  private Map<String, Pair<String, SnapshotIndex>> setupRemoteAndLocalSnapshots(boolean addPrevCheckpoints) throws IOException {
    testBlobStore = new HashMap<>(); // reset blob store
    Map<String, Pair<String, SnapshotIndex>> indexBlobIdAndRemoteAndLocalSnapshotMap = new HashMap<>();
    List<String> localSnapshots = new ArrayList<>();
    List<String> previousRemoteSnapshots = new ArrayList<>();

    localSnapshots.add("[a, c, z/1, y/2, p/m/3, q/n/4]");
    previousRemoteSnapshots.add("[a, b, z/1, x/5, p/m/3, r/o/6]");

    localSnapshots.add("[a, c, z/1, y/1, p/m/1, q/n/1]");
    previousRemoteSnapshots.add("[a, z/1, p/m/1]");

    localSnapshots.add("[z/i/1, y/j/1]");
    previousRemoteSnapshots.add("[z/i/1, x/k/1]");

    // setup local and corresponding remote snapshots
    for (int i = 0; i < localSnapshots.size(); i++) {
      Path localSnapshot = BlobStoreTestUtil.createLocalDir(localSnapshots.get(i));
      String testLocalSnapshot = localSnapshot.toAbsolutePath().toString();
      DirIndex dirIndex = BlobStoreTestUtil.createDirIndex(localSnapshots.get(i));
      SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, testLocalSnapshot);
      Optional<String> prevCheckpointId = Optional.empty();
      if (addPrevCheckpoints) {
        prevCheckpointId = Optional.of(prevSnapshotIndexBlobId + "-" + i);
        DirIndex prevDirIndex = BlobStoreTestUtil.createDirIndex(previousRemoteSnapshots.get(i));
        testBlobStore.put(prevCheckpointId.get(),
            new SnapshotIndex(clock.currentTimeMillis(), snapshotMetadata, prevDirIndex, Optional.empty()));
      }
      SnapshotIndex testRemoteSnapshot =
          new SnapshotIndex(clock.currentTimeMillis(), snapshotMetadata, dirIndex, prevCheckpointId);
      indexBlobIdAndRemoteAndLocalSnapshotMap.put("blobId-" + i, Pair.of(testLocalSnapshot, testRemoteSnapshot));
      testBlobStore.put("blobId-" + i, testRemoteSnapshot);
    }
    return indexBlobIdAndRemoteAndLocalSnapshotMap;
  }

  private class MockBlobStoreBackupManager extends BlobStoreBackupManager {

    public MockBlobStoreBackupManager(JobModel jobModel, ContainerModel containerModel, TaskModel taskModel,
        ExecutorService backupExecutor, BlobStoreBackupManagerMetrics blobStoreTaskBackupMetrics, Config config,
        File loggedStoreBaseDir, StorageManagerUtil storageManagerUtil,
        BlobStoreManager blobStoreManager) {
      super(jobModel, containerModel, taskModel, backupExecutor, blobStoreTaskBackupMetrics, config, clock,
          loggedStoreBaseDir, storageManagerUtil, blobStoreManager);
    }

    @Override
    protected BlobStoreUtil createBlobStoreUtil(BlobStoreManager blobStoreManager, ExecutorService executor,
        BlobStoreConfig blobStoreConfig, BlobStoreBackupManagerMetrics metrics) {
      return blobStoreUtil;
    }
  }
}
