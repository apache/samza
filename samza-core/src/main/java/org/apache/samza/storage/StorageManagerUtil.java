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
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.samza.clustermanager.StandbyTaskUtil;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.FileUtil;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StorageManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StorageManagerUtil.class);
  public static final String OFFSET_FILE_NAME_NEW = "OFFSET-v1.1";
  public static final String OFFSET_FILE_NAME_LEGACY = "OFFSET";
  public static final String SIDE_INPUT_OFFSET_FILE_NAME_LEGACY = "SIDE-INPUT-OFFSETS";
  private static final ObjectMapper OBJECT_MAPPER = SamzaObjectMapper.getObjectMapper();
  private static final TypeReference<Map<SystemStreamPartition, String>> OFFSETS_TYPE_REFERENCE =
            new TypeReference<Map<SystemStreamPartition, String>>() { };
  private static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writerWithType(OFFSETS_TYPE_REFERENCE);


  /**
   * Fetch the starting offset for the input {@link SystemStreamPartition}
   *
   * Note: The method doesn't respect {@link org.apache.samza.config.StreamConfig#CONSUMER_OFFSET_DEFAULT()} and
   * {@link org.apache.samza.config.StreamConfig#CONSUMER_RESET_OFFSET()} configurations. It will use the locally
   * checkpointed offset if it is valid, or fall back to oldest offset of the stream.
   *
   * @param ssp system stream partition for which starting offset is requested
   * @param admin system admin associated with the ssp
   * @param fileOffset local file offset for the ssp
   * @param oldestOffset oldest offset for the ssp from the source
   * @return starting offset for the incoming {@link SystemStreamPartition}
   */
  public static String getStartingOffset(
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

    return startingOffset;
  }

  /**
   * Checks if the store is stale. If the time elapsed since the last modified time of the offset file is greater than
   * the {@code storeDeleteRetentionInMs}, then the store is considered stale.
   *
   * @param storeDir the base directory of the store
   * @param storeDeleteRetentionInMs store delete retention in millis
   * @param currentTimeMs current time in ms
   * @return true if the store is stale, false otherwise
   */
  public static boolean isStaleStore(File storeDir, long storeDeleteRetentionInMs, long currentTimeMs) {
    long offsetFileLastModifiedTime;
    boolean isStaleStore = false;
    String storePath = storeDir.toPath().toString();

    if (storeDir.exists()) {

      // We check if the new offset-file exists, if so we use its last-modified time, if it doesn't we use the legacy file,
      // if neither exists, we use 0L (the defauilt return value of lastModified() when file does not exist
      File offsetFileRefNew = new File(storeDir, OFFSET_FILE_NAME_NEW);
      File offsetFileRefLegacy = new File(storeDir, OFFSET_FILE_NAME_LEGACY);

      if (offsetFileRefNew.exists()) {
        offsetFileLastModifiedTime = offsetFileRefNew.lastModified();
      } else if(offsetFileRefLegacy.exists()) {
        offsetFileLastModifiedTime = offsetFileRefLegacy.lastModified();
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
   * An offset file associated with logged store {@code storeDir} is valid if it exists and is not empty.
   *
   * @param storeDir the base directory of the store
   * @param storeSSPs storeSSPs (if any) associated with the store
   * @return true if the offset file is valid. false otherwise.
   */
  public static boolean isOffsetFileValid(File storeDir, Set<SystemStreamPartition> storeSSPs) {
    boolean hasValidOffsetFile = false;
    if (storeDir.exists()) {
      Map<SystemStreamPartition, String> offsetContents = readOffsetFile(storeDir, storeSSPs);
      if (offsetContents != null && !offsetContents.isEmpty() && offsetContents.keySet().equals(storeSSPs)) {
        hasValidOffsetFile = true;
      } else {
        LOG.info("Offset file is not valid for store: {}.", storeDir.toPath());
      }
    }

    return hasValidOffsetFile;
  }

  /**
   * Write the given SSP-Offset map into the offsets file.
   * @param storeBaseDir the base directory of the store
   * @param storeName the store name to use
   * @param taskName the task name which is referencing the store
   * @param offsets The SSP-offset to write
   * @param isSideInput true if store is a side-input, false if it is a regular store
   * @throws IOException because of deserializing to json
   */
  public static void writeOffsetFile(File storeBaseDir, String storeName, TaskName taskName, TaskMode taskMode,
      Map<SystemStreamPartition, String> offsets, boolean isSideInput) throws IOException {

    // First, we write the new-format offset file
    File offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, taskMode), OFFSET_FILE_NAME_NEW);
    String fileContents = OBJECT_WRITER.writeValueAsString(offsets);
    FileUtil.writeWithChecksum(offsetFile, fileContents);

    // Now we write the old format offset file, which are different for store-offset and side-inputs
    if (isSideInput) {
      offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, taskMode), SIDE_INPUT_OFFSET_FILE_NAME_LEGACY);
      fileContents = OBJECT_WRITER.writeValueAsString(offsets);
      FileUtil.writeWithChecksum(offsetFile, fileContents);
    } else {
      offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, taskMode), OFFSET_FILE_NAME_LEGACY);
      FileUtil.writeWithChecksum(offsetFile, offsets.entrySet().iterator().next().getValue());
    }
  }

  /**
   *  Delete the offset file for this task and store, if one exists.
   * @param storeBaseDir the base directory of the store
   * @param storeName the store name to use
   * @param taskName the task name which is referencing the store
   */
  public static void deleteOffsetFile(File storeBaseDir, String storeName, TaskName taskName) {
    deleteOffsetFile(storeBaseDir, storeName, taskName, OFFSET_FILE_NAME_NEW);
    deleteOffsetFile(storeBaseDir, storeName, taskName, OFFSET_FILE_NAME_LEGACY);
  }

  /**
   * Delete the given offsetFile for the store if it exists.
   */
  private static void deleteOffsetFile(File storeBaseDir, String storeName, TaskName taskName, String offsetFileName) {
    File offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, TaskMode.Active), offsetFileName);
    if (offsetFile.exists()) {
      FileUtil.rm(offsetFile);
    }
  }

  /**
   * Check if a store's disk footprint exists.
   *
   * @param storeDir the base directory of the store
   * @return true if a non-empty storeDir exists, false otherwise
   */
  public static boolean storeExists(File storeDir) {
    return storeDir.exists() && storeDir.list().length > 0;
  }


  public static Map<SystemStreamPartition, String> readOffsetFile(File storagePartitionDir, Set<SystemStreamPartition> storeSSPs) {

    File offsetFileRefNew = new File(storagePartitionDir, OFFSET_FILE_NAME_NEW);
    File offsetFileRefLegacy = new File(storagePartitionDir, OFFSET_FILE_NAME_LEGACY);

    // First we check if the new offset file exists, if it does we read offsets from it regardless of old or new format,
    // if it doesn't exist, we check if the legacy-offset file exists, if so we read offsets from the old file (regardless of the offset format)
    if (offsetFileRefNew.exists()) {
      return readOffsetFile(storagePartitionDir, offsetFileRefNew.getName(), storeSSPs);
    } else if (offsetFileRefLegacy.exists()) {
      return readOffsetFile(storagePartitionDir, offsetFileRefLegacy.getName(), storeSSPs);
    } else {
      return new HashMap<>();
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
  private static Map<SystemStreamPartition, String> readOffsetFile(File storagePartitionDir, String offsetFileName, Set<SystemStreamPartition> storeSSPs) {
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    String fileContents = null;
    File offsetFileRef = new File(storagePartitionDir, offsetFileName);
    String storePath = storagePartitionDir.getPath();

    if (offsetFileRef.exists()) {
      LOG.info("Found offset file in storage partition directory: {}", storePath);
      try {
        fileContents = FileUtil.readWithChecksum(offsetFileRef);
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
  public static File getStorePartitionDir(File storeBaseDir, String storeName, TaskName taskName, TaskMode taskMode) {
    TaskName taskNameForDirName = taskName;
    if (taskMode.equals(TaskMode.Standby)) {
      taskNameForDirName =  StandbyTaskUtil.getActiveTaskName(taskName);
    }
    return new File(storeBaseDir, (storeName + File.separator + taskNameForDirName.toString()).replace(' ', '_'));
  }
}
