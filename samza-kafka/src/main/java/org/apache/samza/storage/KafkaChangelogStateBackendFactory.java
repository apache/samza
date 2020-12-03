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
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;


public class KafkaChangelogStateBackendFactory implements StateBackendFactory {
  private static StreamMetadataCache streamCache;
  private static SSPMetadataCache sspCache;

  private static StreamMetadataCache getStreamCache(SystemAdmins admins, Clock clock) {
    if (streamCache == null) {
      streamCache = new StreamMetadataCache(admins, 5000, clock);
    }
    return streamCache;
  }

  private static SSPMetadataCache getSspCache(SystemAdmins admins, Clock clock, Set<SystemStreamPartition> ssps) {
    if (sspCache == null) {
      sspCache = new SSPMetadataCache(admins, Duration.ofSeconds(5), clock, ssps);
    }
    return sspCache;
  }

  @Override
  public TaskStorageBackupManager getBackupManager(TaskModel taskModel, Map<String, StorageEngine> taskStores,
      Config config) {
    SystemAdmins systemAdmins = new SystemAdmins(config);
    StorageConfig storageConfig = new StorageConfig(config);
    Map<String, SystemStream> storeChangelogs = storageConfig.getStoreChangelogs();

    File defaultFileDir = new File(System.getProperty("user.dir"), "state");
    File loggedStoreBaseDir = SamzaContainer.getLoggedStorageBaseDir(new JobConfig(config), defaultFileDir);

    if (new TaskConfig(config).getTransactionalStateCheckpointEnabled()) {
      return new KafkaTransactionalStateTaskStorageBackupManager(taskModel.getTaskName(),
          taskStores, storeChangelogs, systemAdmins, loggedStoreBaseDir, taskModel.getChangelogPartition(),
          taskModel.getTaskMode(), new StorageManagerUtil());
    } else {
      return new KafkaNonTransactionalStateTaskStorageBackupManager(taskModel.getTaskName(),
          taskStores, storeChangelogs, systemAdmins, loggedStoreBaseDir, taskModel.getChangelogPartition());
    }
  }

  @Override
  public TaskRestoreManager getRestoreManager(TaskModel taskModel, JobModel jobModel, ContainerModel containerModel,
      java.util.Map<String, StorageEngine> taskStores, Config config, Clock clock) {
    SystemAdmins systemAdmins = new SystemAdmins(config);
    Map<String, SystemStream> storeChangelogs = new StorageConfig(config).getStoreChangelogs();
    Map<String, SystemStream> filteredStoreChangelogs = ContainerStorageManager
        .getChangelogSystemStreams(containerModel, storeChangelogs, null);

    File defaultFileDir = new File(System.getProperty("user.dir"), "state");
    File loggedStoreBaseDir = SamzaContainer.getLoggedStorageBaseDir(new JobConfig(config), defaultFileDir);
    File nonLoggedStoreBaseDir = SamzaContainer.getNonLoggedStorageBaseDir(new JobConfig(config), defaultFileDir);

    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      return new TransactionalStateTaskRestoreManager(
          taskModel,
          taskStores,
          filteredStoreChangelogs,
          systemAdmins,
          null, // TODO @dchen have the restore managers create and manage Kafka consume lifecycle
          KafkaChangelogStateBackendFactory
              .getSspCache(systemAdmins, clock, Collections.emptySet()),
          loggedStoreBaseDir,
          nonLoggedStoreBaseDir,
          config,
          clock
      );
    } else {
      return new NonTransactionalStateTaskRestoreManager(
          taskModel,
          filteredStoreChangelogs,
          taskStores,
          systemAdmins,
          KafkaChangelogStateBackendFactory.getStreamCache(systemAdmins, clock),
          null, // TODO  @dchen have the restore managers create and manage Kafka consume lifecycle
          jobModel.maxChangeLogStreamPartitions,
          loggedStoreBaseDir,
          nonLoggedStoreBaseDir,
          config,
          clock);
    }
  }

  @Override
  public TaskStorageAdmin getAdmin() {
    throw new SamzaException("getAdmin() method not supported for KafkaStateBackendFactory");
  }
}
