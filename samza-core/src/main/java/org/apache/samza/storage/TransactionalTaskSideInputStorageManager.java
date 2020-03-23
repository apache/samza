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
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionalTaskSideInputStorageManager extends NonTransactionalTaskSideInputStorageManager implements TaskSideInputStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionalTaskSideInputStorageManager.class);

  public TransactionalTaskSideInputStorageManager(TaskName taskName, TaskMode taskMode,
      StreamMetadataCache streamMetadataCache, File storeBaseDir, Map<String, StorageEngine> sideInputStores,
      Map<String, SideInputsProcessor> storesToProcessor, Map<String, Set<SystemStreamPartition>> storesToSSPs,
      SystemAdmins systemAdmins, Config config, Clock clock) {
    super(taskName, taskMode, streamMetadataCache, storeBaseDir, sideInputStores, storesToProcessor, storesToSSPs,
        systemAdmins, config, clock);
  }

  @Override
  public synchronized void flush() {
    LOG.info("Flushing side inputs for task: {}", getTaskName());
    stores.values().forEach(StorageEngine::flush);
  }

  @Override
  public void checkpoint(String checkpointId, Map<SystemStreamPartition, String> sspOffsetsToCheckpoint) {
    LOG.info("Creating checkpoint for task: {}", getTaskName());
    // TODO create checkpoint
  }

  @Override
  public void removeOldCheckpoints(String latestCheckpointId) {
    LOG.info("Removing checkpoints older than: {} for task: {}", latestCheckpointId, getTaskName());
    // TODO remove old checkpoints
  }
}
