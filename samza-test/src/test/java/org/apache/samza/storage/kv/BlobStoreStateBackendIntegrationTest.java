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

package org.apache.samza.storage.kv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.BlobStoreConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.storage.MyStatefulApplication;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.serde.SnapshotIndexSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.test.util.TestBlobStoreManager;
import org.apache.samza.util.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(value = Parameterized.class)
public class BlobStoreStateBackendIntegrationTest extends BaseStateBackendIntegrationTest {
  @Parameterized.Parameters(name = "hostAffinity={0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  private static final String INPUT_SYSTEM = "kafka";
  private static final String INPUT_TOPIC = "inputTopic";
  private static final String SIDE_INPUT_TOPIC = "sideInputTopic";

  private static final String REGULAR_STORE_NAME = "regularStore";
  private static final String IN_MEMORY_STORE_NAME = "inMemoryStore";
  private static final String SIDE_INPUT_STORE_NAME = "sideInputStore";

  private static final String IN_MEMORY_STORE_CHANGELOG_TOPIC = "inMemoryStoreChangelog";

  private static final String LOGGED_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir"), "logged-store").getAbsolutePath();
  private static final String BLOB_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir"), "blob-store").getAbsolutePath();
  private static final String BLOB_STORE_LEDGER_DIR = new File(BLOB_STORE_BASE_DIR, "ledger").getAbsolutePath();

  private static final Map<String, String> CONFIGS = new HashMap<String, String>() { {
      put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
      put(JobConfig.PROCESSOR_ID, "0");
      put(TaskConfig.GROUPER_FACTORY, "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");

      put(TaskConfig.CHECKPOINT_READ_VERSIONS, "2, 1");
      put(TaskConfig.CHECKPOINT_WRITE_VERSIONS, "1, 2");
      put(TaskConfig.CHECKPOINT_MANAGER_FACTORY, "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory");
      put(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR(), "1");

      put(TaskConfig.COMMIT_MS, "-1"); // manual commit only
      put(TaskConfig.COMMIT_MAX_DELAY_MS, "0"); // Ensure no commits are skipped due to in progress commits

      // override store level state backend for in memory stores to use Kafka changelogs
      put(String.format(StorageConfig.STORE_BACKUP_FACTORIES, IN_MEMORY_STORE_NAME),
          "org.apache.samza.storage.KafkaChangelogStateBackendFactory");
      put(String.format(StorageConfig.STORE_RESTORE_FACTORIES, IN_MEMORY_STORE_NAME),
          "org.apache.samza.storage.KafkaChangelogStateBackendFactory");

      put(StorageConfig.JOB_BACKUP_FACTORIES, "org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory");
      put(StorageConfig.JOB_RESTORE_FACTORIES, "org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory");
      put(BlobStoreConfig.BLOB_STORE_MANAGER_FACTORY, "org.apache.samza.test.util.TestBlobStoreManagerFactory");

      put(JobConfig.JOB_LOGGED_STORE_BASE_DIR, LOGGED_STORE_BASE_DIR);
      put(TestBlobStoreManager.BLOB_STORE_BASE_DIR, BLOB_STORE_BASE_DIR);
      put(TestBlobStoreManager.BLOB_STORE_LEDGER_DIR, BLOB_STORE_LEDGER_DIR);
    } };

  private final boolean hostAffinity;

  public BlobStoreStateBackendIntegrationTest(boolean hostAffinity) {
    this.hostAffinity = hostAffinity;
  }

  @Before
  @Override
  public void setUp() {
    super.setUp();
    // reset static state shared with task between each parameterized iteration
    MyStatefulApplication.resetTestState();
    FileUtil fileUtil = new FileUtil();
    fileUtil.rm(new File(LOGGED_STORE_BASE_DIR)); // always clear local store on startup
    // no need to clear ledger dir since subdir of blob store base dir
    fileUtil.rm(new File(BLOB_STORE_BASE_DIR)); // always clear local "blob store" on startup

  }

  @Test
  public void testStopAndRestart() {
    List<String> inputMessagesOnInitialRun = Arrays.asList("1", "2", "3", "2", "97", "-97", ":98", ":99", ":crash_once");
    List<String> sideInputMessagesOnInitialRun = Arrays.asList("1", "2", "3", "4", "5", "6");
    initialRun(
        INPUT_SYSTEM,
        INPUT_TOPIC,
        SIDE_INPUT_TOPIC,
        inputMessagesOnInitialRun,
        sideInputMessagesOnInitialRun,
        ImmutableSet.of(REGULAR_STORE_NAME),
        Collections.emptyMap(),
        ImmutableSet.of(IN_MEMORY_STORE_NAME),
        ImmutableMap.of(IN_MEMORY_STORE_NAME, IN_MEMORY_STORE_CHANGELOG_TOPIC),
        SIDE_INPUT_STORE_NAME,
        Collections.emptyList(),
        CONFIGS);

    Pair<String, SnapshotIndex> lastRegularSnapshot =
        verifyLedger(REGULAR_STORE_NAME, Optional.empty(), hostAffinity, false, false);
    Pair<String, SnapshotIndex> lastSideInputSnapshot =
        verifyLedger(SIDE_INPUT_STORE_NAME, Optional.empty(), hostAffinity, true,
            false /* no side input offsets file will be present during initial restore */);

    // verifies transactional state too
    List<String> inputMessagesBeforeSecondRun = Arrays.asList("4", "5", "5", ":shutdown");
    List<String> sideInputMessagesBeforeSecondRun = Arrays.asList("7", "8", "9");
    List<String> expectedInitialStoreContentsOnSecondRun = Arrays.asList("1", "2", "3");
    // verifies that in-memory stores backed by changelogs work correctly
    // (requires overriding store level state backends explicitly)
    List<String> expectedInitialInMemoryStoreContentsOnSecondRun = Arrays.asList("1", "2", "3");
    List<String> expectedInitialSideInputStoreContentsOnSecondRun = new ArrayList<>(sideInputMessagesOnInitialRun);
    expectedInitialSideInputStoreContentsOnSecondRun.addAll(sideInputMessagesBeforeSecondRun);
    secondRun(
        hostAffinity,
        LOGGED_STORE_BASE_DIR,
        INPUT_SYSTEM,
        INPUT_TOPIC,
        SIDE_INPUT_TOPIC,
        inputMessagesBeforeSecondRun,
        sideInputMessagesBeforeSecondRun,
        ImmutableSet.of(REGULAR_STORE_NAME),
        Collections.emptyMap(),
        ImmutableSet.of(IN_MEMORY_STORE_NAME),
        ImmutableMap.of(IN_MEMORY_STORE_NAME, IN_MEMORY_STORE_CHANGELOG_TOPIC),
        SIDE_INPUT_STORE_NAME,
        Collections.emptyList(),
        expectedInitialStoreContentsOnSecondRun,
        expectedInitialInMemoryStoreContentsOnSecondRun,
        expectedInitialSideInputStoreContentsOnSecondRun,
        CONFIGS);

    verifyLedger(REGULAR_STORE_NAME, Optional.of(lastRegularSnapshot), hostAffinity, false, false);
    verifyLedger(SIDE_INPUT_STORE_NAME, Optional.of(lastSideInputSnapshot), hostAffinity, true, true);
  }

  /**
   * Verifies the ledger for TestBlobStoreManager.
   * @param startingSnapshot snapshot file name and files present in snapshot at the beginning of verification (from previous run), if any.
   * @return Pair file for latest snapshot at time of verification
   */
  private static Pair<String, SnapshotIndex> verifyLedger(String storeName,
      Optional<Pair<String, SnapshotIndex>> startingSnapshot,
      boolean hostAffinity, boolean verifySideInputOffsetsUploaded, boolean verifySideInputOffsetsRestored) {
    Path ledgerLocation = Paths.get(BLOB_STORE_LEDGER_DIR);
    try {
      File filesAddedLedger = Paths.get(ledgerLocation.toString(), TestBlobStoreManager.LEDGER_FILES_ADDED).toFile();
      Set<String> filesAdded = Files.lines(filesAddedLedger.toPath()).filter(l -> l.contains(storeName)).collect(Collectors.toSet());
      File filesReadLedger = Paths.get(ledgerLocation.toString(), TestBlobStoreManager.LEDGER_FILES_READ).toFile();
      Set<String> filesRead = Files.lines(filesReadLedger.toPath()).filter(l -> l.contains(storeName)).collect(Collectors.toSet());
      File filesDeletedLedger = Paths.get(ledgerLocation.toString(), TestBlobStoreManager.LEDGER_FILES_DELETED).toFile();
      Set<String> filesDeleted = Files.lines(filesDeletedLedger.toPath()).filter(l -> l.contains(storeName)).collect(Collectors.toSet());
      File filesTTLUpdatedLedger = Paths.get(ledgerLocation.toString(), TestBlobStoreManager.LEDGER_FILES_TTL_UPDATED).toFile();
      Set<String> filesTTLUpdated = Files.lines(filesTTLUpdatedLedger.toPath()).filter(l -> l.contains(storeName)).collect(Collectors.toSet());

      // 1. test that files read = files present in last snapshot *at run start* + snapshot file itself + previous snapshot files
      if (startingSnapshot.isPresent() && !hostAffinity) { // no restore if host affinity (local state already present)
        Set<String> filesPresentInStartingSnapshot = startingSnapshot.get().getRight()
            .getDirIndex().getFilesPresent().stream()
            .map(fi -> fi.getBlobs().get(0).getBlobId()).collect(Collectors.toSet());
        Set<String> filesToRestore = new HashSet<>();
        filesToRestore.add(startingSnapshot.get().getLeft());
        filesToRestore.addAll(filesPresentInStartingSnapshot);
        // assert that all files to restore in starting snapshot + starting snapshot itself are present in files read
        assertTrue(Sets.difference(filesToRestore, filesRead).isEmpty());
        // assert that the remaining read files are all snapshot indexes (for post commit cleanup)
        assertTrue(Sets.difference(filesRead, filesToRestore).stream().allMatch(s -> s.contains(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH)));
      }

      // read files added again as ordered list, not set, to get last file added
      List<String> filesAddedLines = Files.readAllLines(filesAddedLedger.toPath()).stream().filter(l -> l.contains(storeName)).collect(Collectors.toList());
      String lastFileAdded = filesAddedLines.get(filesAddedLines.size() - 1); // get last line.
      assertTrue(lastFileAdded.contains(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH)); // assert is a snapshot
      SnapshotIndex lastSnapshotIndex = new SnapshotIndexSerde().fromBytes(Files.readAllBytes(Paths.get(lastFileAdded)));
      Set<String> filesPresentInLastSnapshot = lastSnapshotIndex.getDirIndex().getFilesPresent().stream()
          .map(fi -> fi.getBlobs().get(0).getBlobId()).collect(Collectors.toSet());

      // 2. test that all added files were ttl reset
      assertEquals(filesAdded, filesTTLUpdated);

      // 3. test that files deleted = files added - files present in last snapshot + snapshot file itself
      // i.e., net remaining files (files added - files deleted) = files present in last snapshot + snapshot file itself.
      assertEquals(Sets.difference(filesAdded, filesDeleted),
          Sets.union(filesPresentInLastSnapshot, Collections.singleton(lastFileAdded)));

      // 4. test that the files restored/added for side input stores contains side input offsets file
      if (verifySideInputOffsetsUploaded) {
        assertTrue(filesAdded.stream().anyMatch(f -> f.contains(StorageManagerUtil.SIDE_INPUT_OFFSET_FILE_NAME_LEGACY)));
      }

      if (!hostAffinity && verifySideInputOffsetsRestored) { // only read / restored if no host affinity
        assertTrue(filesRead.stream().anyMatch(f -> f.contains(StorageManagerUtil.SIDE_INPUT_OFFSET_FILE_NAME_LEGACY)));
      }

      return Pair.of(lastFileAdded, lastSnapshotIndex);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class MySideInputProcessor implements SideInputsProcessor, Serializable {
    @Override
    public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
      return ImmutableSet.of(new Entry<>(message.getKey(), message.getMessage()));
    }
  }
}