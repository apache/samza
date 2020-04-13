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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionalTaskSideInputStorageManager extends NonTransactionalTaskSideInputStorageManager implements TaskSideInputStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionalTaskSideInputStorageManager.class);

  public TransactionalTaskSideInputStorageManager(
      TaskName taskName,
      TaskMode taskMode,
      File storeBaseDir,
      Map<String, StorageEngine> sideInputStores,
      Map<String, Set<SystemStreamPartition>> storesToSSPs,
      Clock clock) {
    super(taskName, taskMode, storeBaseDir, sideInputStores, storesToSSPs, clock);
  }

  @Override
  public Map<String, Path> checkpoint(CheckpointId checkpointId) {
    LOG.info("Creating checkpoint for task: {}", this.taskName);

    Map<String, Path> checkpointPaths = new HashMap<>();
    stores.forEach((store, storageEngine) ->
      // TODO what subset of stores to checkpoint? an ssp can be a changelog side input for one store and a regular side input for another store
      storageEngine.checkpoint(checkpointId).ifPresent(path -> checkpointPaths.put(store, path))
    );
    return checkpointPaths;
  }

  @Override
  public void removeOldCheckpoints(String latestCheckpointId) {
    LOG.info("Removing checkpoints older than: {} for task: {}", latestCheckpointId, this.taskName);
    File[] storeDirs = storeBaseDir.listFiles((dir, name) -> stores.containsKey(name));
    (storeDirs == null ? Stream.<File>empty() : Arrays.stream(storeDirs)).forEach(storeDir -> {
        String taskStoreName = storageManagerUtil.getTaskStoreDir(storeBaseDir, storeDir.getName(), taskName, taskMode).getName();
        FileFilter wildcardFileFilter = new WildcardFileFilter(taskStoreName + "-*");
        File[] checkpointDirs = storeDir.listFiles(wildcardFileFilter);
        (checkpointDirs == null ? Stream.<File>empty() : Arrays.stream(checkpointDirs)).forEach(checkpointDir -> {
            if (checkpointDir.getName().contains(latestCheckpointId)) {
              try {
                FileUtils.deleteDirectory(checkpointDir);
              } catch (IOException e) {
                LOG.error("Failed to remove old checkpointDir: {} for task: {}, latestCheckpointId: {}", checkpointDir, taskName, latestCheckpointId);
              }
            }
          });
      });
  }

  /**
   * This method will write offset files to each store's primary and checkpoint directories (if given) corresponding
   * to the set of lastProcessedOffsets given.
   */
  @Override
  public void writeOffsetFiles(Map<SystemStreamPartition, String> lastProcessedOffsets, Map<String, Path> checkpointPaths) {
    // write offset files for each store's non-checkpoint directories
    super.writeOffsetFiles(lastProcessedOffsets, Collections.emptyMap());

    // write offset files to checkpoint directories
    checkpointPaths.forEach((storeName, checkpointDir) -> {
        File taskStoreDir = storageManagerUtil.getTaskStoreDir(storeBaseDir, storeName, taskName, taskMode);
        Map<SystemStreamPartition, String> offsets = lastProcessedOffsets.entrySet().stream()
            .filter(entry -> super.storeToSSPs.get(storeName).contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        try {
          storageManagerUtil.writeOffsetFile(taskStoreDir, offsets, true);
        } catch (IOException e) {
          String errorMsg =
              String.format("Failed to write offset file for store: %s at checkpoint dir: %s", storeName, checkpointDir);
          throw new SamzaException(errorMsg, e);
        }
      });
  }
}
