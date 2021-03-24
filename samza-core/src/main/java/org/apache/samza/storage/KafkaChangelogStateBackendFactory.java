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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.util.Clock;


public class KafkaChangelogStateBackendFactory implements StateBackendFactory {
  // TODO dchen make threadsafe
  private static StreamMetadataCache streamCache;
  private static SSPMetadataCache sspCache;

  @Override
  public TaskBackupManager getBackupManager(JobModel jobModel, ContainerModel containerModel, TaskModel taskModel,
      Config config, Clock clock) {
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
      Map<String, SystemConsumer> storeConsumers,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes,
      MetricsRegistry metricsRegistry,
      MessageCollector messageCollector,
      Set<String> storeNames,
      Config config,
      Clock clock,
      File loggedStoreBaseDir,
      File nonLoggedStoreBaseDir) {
    SystemAdmins systemAdmins = new SystemAdmins(config, this.getClass().getSimpleName());
    Map<String, SystemStream> storeChangelogs = new StorageConfig(config).getStoreChangelogs();
    Set<SystemStreamPartition> changelogSSPs = storeChangelogs.values().stream()
        .flatMap(ss -> containerContext.getContainerModel().getTasks().values().stream()
            .map(tm -> new SystemStreamPartition(ss, tm.getChangelogPartition())))
        .collect(Collectors.toSet());
    Map<String, SystemStream> filteredStoreChangelogs = ContainerStorageManager
        .getChangelogSystemStreams(containerContext.getContainerModel(), storeChangelogs, null); // TODO dchen should be system -> consumers map

    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      return new TransactionalStateTaskRestoreManager(
          storeNames,
          jobContext,
          containerContext,
          taskModel,
          filteredStoreChangelogs,
          storageEngineFactories,
          serdes,
          systemAdmins,
          storeConsumers,
          metricsRegistry,
          messageCollector,
          KafkaChangelogStateBackendFactory
              .getSspCache(systemAdmins, clock, changelogSSPs),
          loggedStoreBaseDir,
          nonLoggedStoreBaseDir,
          config,
          clock
      );
    } else {
      return new NonTransactionalStateTaskRestoreManager(
          storeNames,
          jobContext,
          containerContext,
          taskModel,
          filteredStoreChangelogs,
          storageEngineFactories,
          serdes,
          systemAdmins,
          KafkaChangelogStateBackendFactory.getStreamCache(systemAdmins, clock),
          storeConsumers,
          metricsRegistry,
          messageCollector,
          jobContext.getJobModel().maxChangeLogStreamPartitions,
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
  private static StreamMetadataCache getStreamCache(SystemAdmins admins, Clock clock) {
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
  private static SSPMetadataCache getSspCache(SystemAdmins admins, Clock clock, Set<SystemStreamPartition> ssps) {
    if (sspCache == null) {
      sspCache = new SSPMetadataCache(admins, Duration.ofSeconds(5), clock, ssps);
    }
    return sspCache;
  }
}
