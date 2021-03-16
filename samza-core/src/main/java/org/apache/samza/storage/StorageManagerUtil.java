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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.clustermanager.StandbyTaskUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.serializers.CheckpointV2Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StorageManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StorageManagerUtil.class);
  public static final String CHECKPOINT_FILE_NAME = "CHECKPOINT-V2";
  public static final String OFFSET_FILE_NAME_NEW = "OFFSET-v2";
  public static final String OFFSET_FILE_NAME_LEGACY = "OFFSET";
  public static final String SIDE_INPUT_OFFSET_FILE_NAME_LEGACY = "SIDE-INPUT-OFFSETS";
  private static final ObjectMapper OBJECT_MAPPER = SamzaObjectMapper.getObjectMapper();
  private static final TypeReference<Map<SystemStreamPartition, String>> OFFSETS_TYPE_REFERENCE =
            new TypeReference<Map<SystemStreamPartition, String>>() { };
  private static final ObjectWriter SSP_OFFSET_OBJECT_WRITER = OBJECT_MAPPER.writerFor(OFFSETS_TYPE_REFERENCE);
  private static final String SST_FILE_SUFFIX = ".sst";
  private static final CheckpointV2Serde CHECKPOINT_V2_SERDE = new CheckpointV2Serde();

  /**
   * Returns the path for a storage engine to create its checkpoint based on the current checkpoint id.
   *
   * @param taskStoreDir directory of the store as returned by {@link #getTaskStoreDir}
   * @param checkpointId current checkpoint id
   * @return String denoting the file path of the store with the given checkpoint id
   */
  public static String getCheckpointDirPath(File taskStoreDir, CheckpointId checkpointId) {
    return taskStoreDir.getPath() + "-" + checkpointId.toString();
  }

  /**
   * Fetch the starting offset for the input {@link SystemStreamPartition}
   *
   * Note: The method doesn't respect {@link org.apache.samza.config.StreamConfig#CONSUMER_OFFSET_DEFAULT} and
   * {@link org.apache.samza.config.StreamConfig#CONSUMER_RESET_OFFSET} configurations. It will use the locally
   * checkpointed offset if it is valid, or fall back to oldest offset of the stream.
   *
   * @param ssp system stream partition for which starting offset is requested
   * @param admin system admin associated with the ssp
   * @param fileOffset local file offset for the ssp
   * @param oldestOffset oldest offset for the ssp from the source
   * @return starting offset for the incoming {@link SystemStreamPartition}
   */
  public String getStartingOffset(
      SystemStreamPartition ssp, SystemAdmin admin, String fileOffset, String oldestOffset) {
    String startingOffset = oldestOffset;
    if (fileOffset != null) {
      // File offset was the last message written to the local checkpoint that is also reflected in the store,
      // so we start with the NEXT offset
      String resumeOffset = admin.getOffsetsAfter(ImmutableMap.of(ssp, fileOffset)).get(ssp);
      if (admin.offsetComparator(oldestOffset, resumeOffset) <= 0) {
        startingOffset = resumeOffset;
      } else {
        // If the offset we plan to use is older than the oldest offset, just use the oldest offset.
        // This can happen with source of the store(changelog, etc) configured with a TTL cleanup policy
        LOG.warn("Local store offset {} is lower than the oldest offset {} of the source stream."
            + " The values between these offsets cannot be restored.", resumeOffset, oldestOffset);
      }
    }
    LOG.info("Starting offset for SystemStreamPartition {} is {}, fileOffset: {}, oldestOffset from source: {}", ssp,
        startingOffset, fileOffset, oldestOffset);
    return startingOffset;
  }

  /**
   * Checks if the store is stale. If the time elapsed since the last modified time of the offset file is greater than
   * the {@code storeDeleteRetentionInMs}, then the store is considered stale.
   *
   * @param storeDir the base directory of the store
   * @param storeDeleteRetentionInMs store delete retention in millis
   * @param currentTimeMs current time in ms
   * @param isSideInput true if store is a side-input store, false if it is a regular store
   * @return true if the store is stale, false otherwise
   */
  // TODO BLOCKER dchen do these methods need to be updated to also read the new checkpoint file?
  public boolean isStaleStore(File storeDir, long storeDeleteRetentionInMs, long currentTimeMs, boolean isSideInput) {
    long offsetFileLastModifiedTime;
    boolean isStaleStore = false;
    String storePath = storeDir.toPath().toString();

    if (storeDir.exists()) {

      // We check if the new offset-file exists, if so we use its last-modified time, if it doesn't we use the legacy file
      // depending on if it is a side-input or not,
      // if neither exists, we use 0L (the default return value of lastModified() when file does not exist
      File offsetFileRefNew = new File(storeDir, OFFSET_FILE_NAME_NEW);
      File offsetFileRefLegacy = new File(storeDir, OFFSET_FILE_NAME_LEGACY);
      File sideInputOffsetFileRefLegacy = new File(storeDir, SIDE_INPUT_OFFSET_FILE_NAME_LEGACY);

      if (offsetFileRefNew.exists()) {
        offsetFileLastModifiedTime = offsetFileRefNew.lastModified();
      } else if (!isSideInput && offsetFileRefLegacy.exists()) {
        offsetFileLastModifiedTime = offsetFileRefLegacy.lastModified();
      } else if (isSideInput && sideInputOffsetFileRefLegacy.exists()) {
        offsetFileLastModifiedTime = sideInputOffsetFileRefLegacy.lastModified();
      } else {
        offsetFileLastModifiedTime = 0L;
      }

      if ((currentTimeMs - offsetFileLastModifiedTime) >= storeDeleteRetentionInMs) {
        LOG.info(
            String.format("Store: %s is stale since lastModifiedTime of offset file: %d, is older than store deleteRetentionMs: %d.",
            storePath, offsetFileLastModifiedTime, storeDeleteRetentionInMs));
        isStaleStore = true;
      }
    } else {
      LOG.info("Storage partition directory: {} does not exist.", storePath);
    }
    return isStaleStore;
  }

  /**
   * Directory loggedStoreDir associated with the store storeName is valid if all of the following
   * conditions are true:
   * a) If the store is a persistent store.
   * b) If there is a valid offset file associated with the store.
   * c) If the store has not gone stale.
   *
   * @return true if the logged store is valid, false otherwise.
   */
  public boolean isLoggedStoreValid(String storeName, File loggedStoreDir, Config config,
      Map<String, SystemStream> storeChangelogs, TaskModel taskModel, Clock clock, Map<String, StorageEngine> taskStores) {
    long changeLogDeleteRetentionInMs = new StorageConfig(config).getChangeLogDeleteRetentionInMs(storeName);

    if (storeChangelogs.containsKey(storeName)) {
      SystemStreamPartition changelogSSP = new SystemStreamPartition(
          storeChangelogs.get(storeName), taskModel.getChangelogPartition());

      return taskStores.get(storeName).getStoreProperties().isPersistedToDisk()
          && isOffsetFileValid(loggedStoreDir, Collections.singleton(changelogSSP), false)
          && !isStaleStore(loggedStoreDir, changeLogDeleteRetentionInMs, clock.currentTimeMillis(), false);
    }

    return false;
  }

  /**
   * An offset file associated with logged store {@code storeDir} is valid if it exists, is not empty,
   * and the offsets are for the store's current changelog or side input SSPs.
   *
   * @param storeDir the base directory of the store
   * @param storeSSPs ssps (if any) associated with the store
   * @param isSideInput true if store is a side-input store, false if it is a regular store
   * @return true if the offset file is valid. false otherwise.
   */
  // TODO BLOCKER dchen do these methods need to be updated to also read the new checkpoint file?
  public boolean isOffsetFileValid(File storeDir, Set<SystemStreamPartition> storeSSPs, boolean isSideInput) {
    boolean hasValidOffsetFile = false;
    if (storeDir.exists()) {
      Map<SystemStreamPartition, String> offsetContents = readOffsetFile(storeDir, storeSSPs, isSideInput);
      if (offsetContents == null) {
        LOG.info("Offset file is invalid since it does not exist. Store directory: {}", storeDir.toPath());
      } else if (offsetContents.isEmpty()) {
        LOG.info("Offset file is invalid since it is empty. Store directory: {}", storeDir.toPath());
      } else if (!offsetContents.keySet().equals(storeSSPs)) {
        LOG.info("Offset file is invalid since changelog or side input SSPs don't match. "
            + "Store directory: {}. SSPs from offset-file: {} SSPs expected: {} ",
            storeDir.toPath(), offsetContents.keySet(), storeSSPs);
      } else {
        hasValidOffsetFile = true;
      }
    }

    return hasValidOffsetFile;
  }

  /**
   * Write the given SSP-Offset map into the offsets file.
   * @param storeDir the directory of the store
   * @param offsets The SSP-offset to write
   * @param isSideInput true if store is a side-input store, false if it is a regular store
   * @throws IOException because of deserializing to json
   */
  public void writeOffsetFile(File storeDir, Map<SystemStreamPartition, String> offsets, boolean isSideInput) throws IOException {

    // First, we write the new-format offset file
    File offsetFile = new File(storeDir, OFFSET_FILE_NAME_NEW);
    String fileContents = SSP_OFFSET_OBJECT_WRITER.writeValueAsString(offsets);
    FileUtil fileUtil = new FileUtil();
    fileUtil.writeWithChecksum(offsetFile, fileContents);

    // Now we write the old format offset file, which are different for store-offset and side-inputs
    if (isSideInput) {
      offsetFile = new File(storeDir, SIDE_INPUT_OFFSET_FILE_NAME_LEGACY);
      fileContents = SSP_OFFSET_OBJECT_WRITER.writeValueAsString(offsets);
      fileUtil.writeWithChecksum(offsetFile, fileContents);
    } else {
      offsetFile = new File(storeDir, OFFSET_FILE_NAME_LEGACY);
      fileUtil.writeWithChecksum(offsetFile, offsets.entrySet().iterator().next().getValue());
    }
  }

  /**
   * Writes the checkpoint to the store checkpoint directory based on the checkpointId.
   *
   * @param storeDir store or store checkpoint directory to write the checkpoint to
   * @param checkpoint checkpoint v2 containing the checkpoint Id
   */
  public void writeCheckpointV2File(File storeDir, CheckpointV2 checkpoint) {
    File offsetFile = new File(storeDir, CHECKPOINT_FILE_NAME);
    byte[] fileContents = CHECKPOINT_V2_SERDE.toBytes(checkpoint);
    FileUtil fileUtil = new FileUtil();
    fileUtil.writeWithChecksum(offsetFile, new String(fileContents));
  }

  /**
   * Delete the offset file for this store, if one exists.
   * @param storeDir the directory of the store
   */
  public void deleteOffsetFile(File storeDir) {
    deleteOffsetFile(storeDir, OFFSET_FILE_NAME_NEW);
    deleteOffsetFile(storeDir, OFFSET_FILE_NAME_LEGACY);
  }

  /**
   * Delete the given offsetFile for the store if it exists.
   */
  private void deleteOffsetFile(File storeDir, String offsetFileName) {
    File offsetFile = new File(storeDir, offsetFileName);
    if (offsetFile.exists()) {
      new FileUtil().rm(offsetFile);
    }
  }

  /**
   * Check if a store's disk footprint exists.
   *
   * @param storeDir the base directory of the store
   * @return true if a non-empty storeDir exists, false otherwise
   */
  public boolean storeExists(File storeDir) {
    return storeDir.exists() && storeDir.list().length > 0;
  }

  /**
   * Read and return the offset from the directory's offset file
   *
   * @param storagePartitionDir the base directory of the store
   * @param storeSSPs SSPs associated with the store (if any)
   * @param isSideInput, true if the store is a side-input store, false otherwise
   * @return the content of the offset file if it exists for the store, null otherwise.
   */
  public Map<SystemStreamPartition, String> readOffsetFile(File storagePartitionDir, Set<SystemStreamPartition> storeSSPs, boolean isSideInput) {

    File offsetFileRefNew = new File(storagePartitionDir, OFFSET_FILE_NAME_NEW);
    File offsetFileRefLegacy = new File(storagePartitionDir, OFFSET_FILE_NAME_LEGACY);
    File sideInputOffsetFileRefLegacy = new File(storagePartitionDir, SIDE_INPUT_OFFSET_FILE_NAME_LEGACY);

    // First we check if the new offset file exists, if it does we read offsets from it regardless of old or new format,
    // if it doesn't exist, we check if the store is non-sideInput and legacy-offset file exists, if so we read offsets
    // from the old non-side-input offset file (regardless of the offset format),
    // last, we check if the store is a sideInput and the old side-input-offset file exists
    if (offsetFileRefNew.exists()) {
      return readOffsetFile(storagePartitionDir, offsetFileRefNew.getName(), storeSSPs);
    } else if (!isSideInput && offsetFileRefLegacy.exists()) {
      return readOffsetFile(storagePartitionDir, offsetFileRefLegacy.getName(), storeSSPs);
    } else if (isSideInput && sideInputOffsetFileRefLegacy.exists()) {
      return readOffsetFile(storagePartitionDir, sideInputOffsetFileRefLegacy.getName(), storeSSPs);
    } else {
      return new HashMap<>();
    }
  }

  /**
   * Read and return the {@link CheckpointV2} from the directory's {@link #CHECKPOINT_FILE_NAME} file.
   * If the file does not exist, returns null.
   * // TODO HIGH dchen add tests at all call sites for handling null value.
   *
   * @param storagePartitionDir store directory to read the checkpoint file from
   * @return the {@link CheckpointV2} object retrieved from the checkpoint file if found, otherwise return null
   */
  public CheckpointV2 readCheckpointV2File(File storagePartitionDir) {
    File checkpointFile = new File(storagePartitionDir, CHECKPOINT_FILE_NAME);
    if (checkpointFile.exists()) {
      String serializedCheckpointV2 = new FileUtil().readWithChecksum(checkpointFile);
      return new CheckpointV2Serde().fromBytes(serializedCheckpointV2.getBytes());
    } else {
      return null;
    }
  }

  /**
   * Read and return the contents of the offset file.
   *
   * @param storagePartitionDir the base directory of the store
   * @param offsetFileName the name of the offset file
   * @param storeSSPs SSPs associated with the store (if any)
   * @return the content of the offset file if it exists for the store, null otherwise.
   */
  private Map<SystemStreamPartition, String> readOffsetFile(File storagePartitionDir, String offsetFileName, Set<SystemStreamPartition> storeSSPs) {
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    String fileContents = null;
    File offsetFileRef = new File(storagePartitionDir, offsetFileName);
    String storePath = storagePartitionDir.getPath();

    if (offsetFileRef.exists()) {
      LOG.debug("Found offset file in storage partition directory: {}", storePath);
      try {
        fileContents = new FileUtil().readWithChecksum(offsetFileRef);
        offsets = OBJECT_MAPPER.readValue(fileContents, OFFSETS_TYPE_REFERENCE);
      } catch (JsonParseException | JsonMappingException e) {
        LOG.info("Exception in json-parsing offset file {} {}, reading as string offset-value", storagePartitionDir.toPath(), offsetFileName);
        final String finalFileContents = fileContents;
        offsets = (storeSSPs.size() == 1) ? storeSSPs.stream().collect(Collectors.toMap(ssp -> ssp, offset -> finalFileContents)) : offsets;
      } catch (Exception e) {
        LOG.warn("Failed to read offset file in storage partition directory: {}", storePath, e);
      }
    } else {
      LOG.info("No offset file found in storage partition directory: {}", storePath);
    }

    return offsets;
  }

  /**
   * Creates and returns a File pointing to the directory for the given store and task, given a particular base directory.
   * In case of a standby task (TaskMode.Standby), the storeDirectory is the same as it would be for an active task.
   *
   * @param storeBaseDir the base directory to use
   * @param storeName the store name to use
   * @param taskName the task name which is referencing the store
   * @param taskMode the mode of the given task
   * @return the partition directory for the store
   */
  public File getTaskStoreDir(File storeBaseDir, String storeName, TaskName taskName, TaskMode taskMode) {
    TaskName taskNameForDirName = taskName;
    if (taskMode.equals(TaskMode.Standby)) {
      taskNameForDirName =  StandbyTaskUtil.getActiveTaskName(taskName);
    }
    return new File(storeBaseDir, (storeName + File.separator + taskNameForDirName.toString()).replace(' ', '_'));
  }

  public List<File> getTaskStoreCheckpointDirs(File storeBaseDir, String storeName,
      TaskName taskName, TaskMode taskMode) {
    try {
      File storeDir = new File(storeBaseDir, storeName);
      String taskStoreName = getTaskStoreDir(storeBaseDir, storeName, taskName, taskMode).getName();

      if (storeDir.exists()) { // new store or no local state
        List<File> checkpointDirs = Files.list(storeDir.toPath())
            .map(Path::toFile)
            .filter(file -> file.getName().contains(taskStoreName + "-"))
            .collect(Collectors.toList());
        return checkpointDirs;
      } else {
        return Collections.emptyList();
      }
    } catch (IOException e) {
      throw new SamzaException(
          String.format("Error finding checkpoint dirs for task: %s mode: %s store: %s in dir: %s",
              taskName, taskMode, storeName, storeBaseDir), e);
    }
  }

  public void restoreCheckpointFiles(File checkpointDir, File storeDir) {
    // the current task store dir should already be deleted for restore
    assert !storeDir.exists();

    try {
      Files.createDirectory(storeDir.toPath());

      for (File file : checkpointDir.listFiles()) {
        String fileName = file.getName();
        File targetFile = new File(storeDir, fileName);

        if (fileName.endsWith(SST_FILE_SUFFIX)) {
          // hardlink'ing the immutable sst-files.
          Files.createLink(targetFile.toPath(), file.toPath());
        } else {
          // true copy for all other files.
          Files.copy(file.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (Exception e) {
      throw new SamzaException(String.format(
          "Failed to restore store from checkpoint dir: %s to current dir: %s", checkpointDir, storeDir), e);
    }
  }
}
