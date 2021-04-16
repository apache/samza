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
import java.util.concurrent.ExecutorService;
import org.apache.samza.config.Config;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.Clock;


/**
 * Factory to build the Samza {@link TaskBackupManager}, {@link TaskRestoreManager} and {@link TaskStorageAdmin}
 * for a particular state storage backend, which are used to durably backup the Samza task state.
 */
public interface StateBackendFactory {
  TaskBackupManager getBackupManager(JobModel jobModel,
      ContainerModel containerModel,
      TaskModel taskModel,
      ExecutorService backupExecutor,
      MetricsRegistry taskInstanceMetricsRegistry,
      Config config,
      Clock clock);

  TaskRestoreManager getRestoreManager(JobContext jobContext,
      ContainerContext containerContext,
      TaskModel taskModel,
      ExecutorService restoreExecutor,
      MetricsRegistry metricsRegistry,
      Config config,
      Clock clock,
      File loggedStoreBaseDir,
      File nonLoggedStoreBaseDir,
      KafkaChangelogRestoreParams kafkaChangelogRestoreParams);

  TaskStorageAdmin getAdmin();
}
