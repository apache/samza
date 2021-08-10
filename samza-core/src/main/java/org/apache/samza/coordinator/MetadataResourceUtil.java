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

package org.apache.samza.coordinator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.ChangelogStreamManager;


/**
 * Loads the managers responsible for the creation and loading of metadata related resources.
 */
// TODO: Replace with a metadata admin interface when the {@link MetadataStore} is fully augmented to handle all metadata sources.
public class MetadataResourceUtil {
  private final CheckpointManager checkpointManager;
  private final Config config;
  private final JobModel jobModel; // TODO: Should be loaded by metadata store in the future

  /**
   * @param jobModel the loaded {@link JobModel}
   * @param metricsRegistry the registry for reporting metrics.
   */
  public MetadataResourceUtil(JobModel jobModel, MetricsRegistry metricsRegistry, Config config) {
    this.config = config;
    this.jobModel = jobModel;
    TaskConfig taskConfig = new TaskConfig(config);
    this.checkpointManager = taskConfig.getCheckpointManager(metricsRegistry).orElse(null);
  }

  /**
   * Creates and loads the required metadata resources for checkpoints, changelog stream and other
   * resources related to the metadata system
   */
  public void createResources() {
    if (checkpointManager != null) {
      checkpointManager.createResources();
    }
    createChangelogStreams();
  }

  @VisibleForTesting
  void createChangelogStreams() {
    ChangelogStreamManager.createChangelogStreams(config, jobModel.getMaxChangeLogStreamPartitions());
  }

  @VisibleForTesting
  CheckpointManager getCheckpointManager() {
    return checkpointManager;
  }
}
