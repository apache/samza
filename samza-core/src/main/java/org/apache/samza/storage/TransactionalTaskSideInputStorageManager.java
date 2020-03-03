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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    return new HashMap<>();
  }

  @Override
  public void removeOldCheckpoints(String latestCheckpointId) {

  }

  /**
   * This method will write offset files to each store's primary and checkpoint directories (if given) corresponding
   * to the set of lastProcessedOffsets given.
   */
  @Override
  public void writeOffsetFiles(Map<SystemStreamPartition, String> lastProcessedOffsets, Map<String, Path> checkpointPaths) {

  }
}
