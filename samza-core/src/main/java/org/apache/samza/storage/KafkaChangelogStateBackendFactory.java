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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;


public class KafkaChangelogStateBackendFactory implements StateBackendFactory {
  private StreamMetadataCache streamCache;
  private SSPMetadataCache sspCache;

  @Override
  public TaskBackupManager getBackupManager(JobModel jobModel,
      ContainerModel containerModel,
      TaskModel taskModel,
      ExecutorService backupExecutor,
      MetricsRegistry metricsRegistry,
      Config config,
      Clock clock) {
    SystemAdmins systemAdmins = new SystemAdmins(config);
    StorageConfig storageConfig = new StorageConfig(config);
    Map<String, SystemStream> storeChangelogs = storageConfig.getStoreChangelogs();

    if (new TaskConfig(config).getTransactionalStateCheckpointEnabled()) {
      return new KafkaTransactionalStateTaskBackupManager(taskModel.getTaskName(), storeChangelogs,
          systemAdmins, taskModel.getChangelogPartition());
    } else {
      return new KafkaNonTransactionalStateTaskBackupManager(taskModel.getTaskName(), storeChangelogs,
          systemAdmins, taskModel.getChangelogPartition());
    }
  }

  @Override
  public TaskRestoreManager getRestoreManager(JobContext jobContext,
      ContainerContext containerContext,
      TaskModel taskModel,
      ExecutorService restoreExecutor,
      MetricsRegistry metricsRegistry,
      Config config,
      Clock clock,
      File loggedStoreBaseDir,
      File nonLoggedStoreBaseDir,
      KafkaChangelogRestoreParams kafkaChangelogRestoreParams) {
    Map<String, SystemStream> storeChangelogs = new StorageConfig(config).getStoreChangelogs();
    Set<SystemStreamPartition> changelogSSPs = storeChangelogs.values().stream()
        .flatMap(ss -> containerContext.getContainerModel().getTasks().values().stream()
            .map(tm -> new SystemStreamPartition(ss, tm.getChangelogPartition())))
        .collect(Collectors.toSet());
    // filter out standby store-ssp pairs
    Map<String, SystemStream> filteredStoreChangelogs =
        filterStandbySystemStreams(storeChangelogs, containerContext.getContainerModel());
    SystemAdmins systemAdmins = new SystemAdmins(kafkaChangelogRestoreParams.getSystemAdmins());

    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      return new TransactionalStateTaskRestoreManager(
          kafkaChangelogRestoreParams.getStoreNames(),
          jobContext,
          containerContext,
          taskModel,
          filteredStoreChangelogs,
          kafkaChangelogRestoreParams.getInMemoryStores(),
          kafkaChangelogRestoreParams.getStorageEngineFactories(),
          kafkaChangelogRestoreParams.getSerdes(),
          systemAdmins,
          kafkaChangelogRestoreParams.getStoreConsumers(),
          metricsRegistry,
          kafkaChangelogRestoreParams.getCollector(),
          getSspCache(systemAdmins, clock, changelogSSPs),
          loggedStoreBaseDir,
          nonLoggedStoreBaseDir,
          config,
          clock
      );
    } else {
      return new NonTransactionalStateTaskRestoreManager(
          kafkaChangelogRestoreParams.getStoreNames(),
          jobContext,
          containerContext,
          taskModel,
          filteredStoreChangelogs,
          kafkaChangelogRestoreParams.getInMemoryStores(),
          kafkaChangelogRestoreParams.getStorageEngineFactories(),
          kafkaChangelogRestoreParams.getSerdes(),
          systemAdmins,
          getStreamCache(systemAdmins, clock),
          kafkaChangelogRestoreParams.getStoreConsumers(),
          metricsRegistry,
          kafkaChangelogRestoreParams.getCollector(),
          jobContext.getJobModel().getMaxChangeLogStreamPartitions(),
          loggedStoreBaseDir,
          nonLoggedStoreBaseDir,
          config,
          clock
      );
    }
  }

  @Override
  public TaskStorageAdmin getAdmin() {
    throw new SamzaException("getAdmin() method not supported for KafkaStateBackendFactory");
  }

  /**
   * Shared cache across all KafkaRestoreManagers for the Kafka topic
   *
   * @param admins system admins used the fetch the stream metadata
   * @param clock for cache invalidation
   * @return StreamMetadataCache containing the stream metadata
   */
  private StreamMetadataCache getStreamCache(SystemAdmins admins, Clock clock) {
    if (streamCache == null) {
      streamCache = new StreamMetadataCache(admins, 5000, clock);
    }
    return streamCache;
  }

  /**
   * Shared cache across KafkaRestoreManagers for the Kafka partition
   *
   * @param admins system admins used the fetch the stream metadata
   * @param clock for cache invalidation
   * @param ssps SSPs to prefetch
   * @return SSPMetadataCache containing the partition metadata
   */
  private SSPMetadataCache getSspCache(SystemAdmins admins, Clock clock, Set<SystemStreamPartition> ssps) {
    if (sspCache == null) {
      sspCache = new SSPMetadataCache(admins, Duration.ofSeconds(5), clock, ssps);
    }
    return sspCache;
  }

  private Map<String, SystemStream> filterStandbySystemStreams(Map<String, SystemStream> changelogSystemStreams,
      ContainerModel containerModel) {
    Map<SystemStreamPartition, String> changelogSSPToStore = new HashMap<>();
    changelogSystemStreams.forEach((storeName, systemStream) ->
        containerModel.getTasks().forEach((taskName, taskModel) ->
            changelogSSPToStore.put(new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), storeName))
    );

    Set<TaskModel> standbyTaskModels = containerModel.getTasks().values().stream()
        .filter(taskModel -> taskModel.getTaskMode().equals(TaskMode.Standby))
        .collect(Collectors.toSet());

    // remove all standby task changelog ssps
    standbyTaskModels.forEach((taskModel) -> {
      changelogSystemStreams.forEach((storeName, systemStream) -> {
        SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
        changelogSSPToStore.remove(ssp);
      });
    });

    // changelogSystemStreams correspond only to active tasks (since those of standby-tasks moved to sideInputs above)
    return MapUtils.invertMap(changelogSSPToStore).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getSystemStream()));
  }
}
