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

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;


/**
 * Loads the managers responsible for the creation and loading of metadata related resources.
 * TODO: Replace with a metadata admin interface when the {@link MetadataStore} is fully augmented to handle all metadata sources.
 */
public class MetadataResourceManager {
  private final Config config;
  private final CheckpointManager checkpointManager;
  private final ChangelogStreamManager changelogStreamManager;
  private final StartpointManager startpointManager;
  private final JobModel jobModel; // TODO: Should be loaded by metadata store in the future

  /**
   * @param metadataStore the {@link MetadataStore} that manages the metadata. Assumption is that the metadataStore is
   *                      initialized and not stopped.
   * @param jobModel the loaded {@link JobModel}
   * @param metricsRegistry the registry for reporting metrics.
   * @param configOverride the config override to use instead of the one in the jobModel.
   */
  public MetadataResourceManager(MetadataStore metadataStore, JobModel jobModel, MetricsRegistry metricsRegistry, Config configOverride) {
    this.config = configOverride;
    this.jobModel = jobModel;
    this.checkpointManager = new TaskConfigJava(config).getCheckpointManager(metricsRegistry);
    this.changelogStreamManager = metadataStore == null
        ? null
        : new ChangelogStreamManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
    this.startpointManager = metadataStore == null
        ? null
        : new StartpointManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, StartpointManager.NAMESPACE));
  }

  /**
   * @param metadataStore the {@link MetadataStore} that manages the metadata
   * @param jobModel the loaded {@link JobModel}
   * @param metricsRegistry the registry for reporting metrics.
   */
  public MetadataResourceManager(MetadataStore metadataStore, JobModel jobModel, MetricsRegistry metricsRegistry) {
    this(metadataStore, jobModel, metricsRegistry, jobModel.getConfig());
  }

  /**
   * Creates and loads the required metadata resources for checkpoints, changelog stream and other
   * resources related to the metadata system
   */
  public void createResources() {
    CheckpointManager checkpointManager = getCheckpointManager();
    if (checkpointManager != null) {
      checkpointManager.createResources();
    }
    createChangelogStreams();
  }

  public CheckpointManager getCheckpointManager() {
    return checkpointManager;
  }

  public ChangelogStreamManager getChangelogStreamManager() {
    return changelogStreamManager;
  }

  public StartpointManager getStartpointManager() {
    return startpointManager;
  }

  void createChangelogStreams() {
    ChangelogStreamManager.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions);
  }
}
