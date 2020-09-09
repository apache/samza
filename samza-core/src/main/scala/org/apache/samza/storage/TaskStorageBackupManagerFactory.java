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

import scala.collection.immutable.Map;

import java.io.File;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;

public class TaskStorageBackupManagerFactory {
  public static TaskStorageBackupManager create(TaskName taskName, ContainerStorageManager containerStorageManager,
      Map<String, SystemStream> storeChangelogs, SystemAdmins systemAdmins,
      File loggedStoreBaseDir, Partition changelogPartition,
      Config config, TaskMode taskMode) {
    if (new TaskConfig(config).getTransactionalStateCheckpointEnabled()) {
      return new KafkaTransactionalStateTaskStorageBackupManager(taskName, containerStorageManager, storeChangelogs, systemAdmins,
          loggedStoreBaseDir, changelogPartition, taskMode, new StorageManagerUtil());
    } else {
      return new KafkaNonTransactionalStateTaskStorageBackupManager(taskName, containerStorageManager, storeChangelogs, systemAdmins,
          loggedStoreBaseDir, changelogPartition);
    }
  }
}
