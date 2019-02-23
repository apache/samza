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
  public static final String OFFSET_FILE_NAME = "OFFSET";
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
    boolean isStaleStore = false;
    String storePath = storeDir.toPath().toString();
    if (storeDir.exists()) {
      File offsetFileRef = new File(storeDir, OFFSET_FILE_NAME);
      long offsetFileLastModifiedTime = offsetFileRef.lastModified();
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
   * @throws IOException because of deserializing to json
   */
  public static void writeOffsetFile(File storeBaseDir, String storeName, TaskName taskName, TaskMode taskMode,
      Map<SystemStreamPartition, String> offsets) throws IOException {
    File offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, taskMode), OFFSET_FILE_NAME);
    String fileContents = OBJECT_WRITER.writeValueAsString(offsets);
    FileUtil.writeWithChecksum(offsetFile, fileContents);
  }

  /**
   *  Delete the offset file for this task and store, if one exists.
   * @param storeBaseDir the base directory of the store
   * @param storeName the store name to use
   * @param taskName the task name which is referencing the store
   */
  public static void deleteOffsetFile(File storeBaseDir, String storeName, TaskName taskName) {
    File offsetFile = new File(getStorePartitionDir(storeBaseDir, storeName, taskName, TaskMode.Active), OFFSET_FILE_NAME);
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

  /**
   * Read and return the contents of the offset file.
   *
   * @param storagePartitionDir the base directory of the store
   * @param storeSSPs SSPs associated with the store (if any)
   * @return the content of the offset file if it exists for the store, null otherwise.
   */
  public static Map<SystemStreamPartition, String> readOffsetFile(File storagePartitionDir, Set<SystemStreamPartition> storeSSPs) {
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    String fileContents = null;
    File offsetFileRef = new File(storagePartitionDir, OFFSET_FILE_NAME);
    String storePath = storagePartitionDir.getPath();

    if (offsetFileRef.exists()) {
      LOG.info("Found offset file in storage partition directory: {}", storePath);
      try {
        fileContents = FileUtil.readWithChecksum(offsetFileRef);
        offsets = OBJECT_MAPPER.readValue(fileContents, OFFSETS_TYPE_REFERENCE);
      } catch (JsonParseException | JsonMappingException e) {
        LOG.info("Exception in json-parsing offset file {} {}, reading as string offset-value", storagePartitionDir.toPath(), OFFSET_FILE_NAME);
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
   *
   * @param storeBaseDir the base directory to use
   * @param storeName the store name to use
   * @param taskName the task name which is referencing the store
   * @param taskMode
   * @return the partition directory for the store
   */
  public static File getStorePartitionDir(File storeBaseDir, String storeName, TaskName taskName, TaskMode taskMode) {
    // TODO: use task-Mode to decide the storePartitionDir -- standby's dir should be the same as active
    return new File(storeBaseDir, (storeName + File.separator + taskName.toString()).replace(' ', '_'));
  }
}
