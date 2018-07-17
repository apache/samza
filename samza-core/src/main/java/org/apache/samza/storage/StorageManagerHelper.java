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
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class StorageManagerHelper {
  private static final Logger LOG = LoggerFactory.getLogger(StorageManagerHelper.class);

  /**
   * Fetch the starting offset for the input {@link SystemStreamPartition}
   *
   * Note: The method doesn't respect {@link org.apache.samza.config.StreamConfig#CONSUMER_OFFSET_DEFAULT()} and
   * {@link org.apache.samza.config.StreamConfig#CONSUMER_RESET_OFFSET()} configurations and will use the locally
   * checkpointed offset if its valid or fallback to oldest offset of the stream.
   *
   * @param ssp system stream partition for which starting offset is requested
   * @param admin system admin associated with the ssp
   * @param fileOffset local file offset for the ssp
   * @param oldestOffset oldest offset for the ssp from the source
   *
   * @return starting offset for the incoming {@link SystemStreamPartition}
   */
  public String getStartingOffset(SystemStreamPartition ssp, SystemAdmin admin, String fileOffset, String oldestOffset) {
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
   * Checks if the store is stale. IF the time elapsed since the last modified time of the offset file is greater than
   * the {@code storeDeleteRetentionInMs}, then the store is considered stale. For stale stores, we ignore the locally
   * checkpointed offsets and go with the oldest offset from the source.
   *
   * @param storeDir the base directory of the store
   * @param offsetFileName the offset file name
   * @param storeDeleteRetentionInMs store delete retention in millis
   * @param currentTimeInMs current time in millis
   *
   * @return true if the store is stale, false otherwise
   */
  public boolean isStaleStore(File storeDir, String offsetFileName, long storeDeleteRetentionInMs, long currentTimeInMs) {
    boolean isStaleStore = false;
    String storePath = storeDir.toPath().toString();
    if (storeDir.exists()) {
      File offsetFileRef = new File(storeDir, offsetFileName);
      long offsetFileLastModifiedTime = offsetFileRef.lastModified();
      if ((currentTimeInMs - offsetFileLastModifiedTime) >= storeDeleteRetentionInMs) {
        LOG.info(
            String.format("Store: %s is stale since lastModifiedTime of offset file: %d, is older than store deleteRetentionMs: %d.",
            storePath, offsetFileLastModifiedTime, storeDeleteRetentionInMs));
        isStaleStore = true;
      }
    } else {
      LOG.info("Storage partition directory: {} does not exist.", storePath);
      LOG.info("{}", storePath, storePath);
    }
    return isStaleStore;
  }

  /**
   * An offset file associated with logged store {@code storeDir} is valid if it exists and is not empty.
   *
   * @param storeDir the base directory of the store
   * @param offsetFileName name of the offset file
   *
   * @return true if the offset file is valid. false otherwise.
   */
  public boolean isOffsetFileValid(File storeDir, String offsetFileName) {
    boolean hasValidOffsetFile = false;
    if (storeDir.exists()) {
      String offsetContents = readOffsetFile(storeDir, offsetFileName);
      if (offsetContents != null && !offsetContents.isEmpty()) {
        hasValidOffsetFile = true;
      } else {
        LOG.info("Offset file is not valid for store: {}.", storeDir.toPath());
      }
    }

    return hasValidOffsetFile;
  }

  /**
   * Read and return the contents of the offset file.
   *
   * @param storagePartitionDir the base directory of the store
   * @param offsetFileName name of the offset file
   *
   * @return the content of the offset file if it exists for the store, null otherwise.
   */
  public String readOffsetFile(File storagePartitionDir, String offsetFileName) {
    String offset = null;
    File offsetFileRef = new File(storagePartitionDir, offsetFileName);
    String storePath = storagePartitionDir.getPath();

    if (offsetFileRef.exists()) {
      LOG.info("Found offset file in storage partition directory: {}", storePath);
      offset = FileUtil.readWithChecksum(offsetFileRef);
    } else {
      LOG.info("No offset file found in storage partition directory: {}", storePath);
    }

    return offset;
  }
}
