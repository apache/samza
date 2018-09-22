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
package org.apache.samza.standalone;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.grouper.task.GrouperContext;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.runtime.LocationIdProvider;
import org.apache.samza.runtime.LocationIdProviderFactory;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.HashMap;

/**
 * Standalone Job Coordinator does not implement any leader elector module or cluster manager
 *
 * It generates the JobModel using the Config passed into the constructor.
 *
 * Since the standalone JobCoordinator does not perform partition management, it allows two kinds of partition
 * distribution mechanism:
 * <ul>
 *   <li>
 *     Consumer-managed Partition Distribution - For example, using the kafka consumer which also handles partition
 *   load balancing across its consumers. In such a case, all input SystemStreamPartition(s) can be grouped to the same
 *   task instance using {@link org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory} and the
 *   task can be added to a single container using
 *   {@link org.apache.samza.container.grouper.task.SingleContainerGrouperFactory}.
 *   </li>
 *   <li>
 *     User-defined Fixed Partition Distribution - For example, the application may always run a fixed number of
 *   processors and use a static distribution of partitions that doesn't change. This can be achieved by adding custom
 *   {@link org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper} and
 *   {@link org.apache.samza.container.grouper.task.TaskNameGrouper}.
 *   </li>
 * </ul>
 * */
public class PassthroughJobCoordinator implements JobCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PassthroughJobCoordinator.class);
  private final String processorId;
  private final Config config;
  private final LocationId locationId;
  private JobCoordinatorListener coordinatorListener = null;

  public PassthroughJobCoordinator(Config config) {
    this.processorId = createProcessorId(config);
    this.config = config;
    LocationIdProviderFactory locationIdProviderFactory = Util.getObj(new JobConfig(config).getLocationIdProviderFactory(), LocationIdProviderFactory.class);
    LocationIdProvider locationIdProvider = locationIdProviderFactory.getLocationIdProvider(config);
    this.locationId = locationIdProvider.getLocationId();
  }

  @Override
  public void start() {
    // No-op
    JobModel jobModel = null;
    try {
      jobModel = getJobModel();
      CheckpointManager checkpointManager = new TaskConfigJava(jobModel.getConfig()).getCheckpointManager(null);
      if (checkpointManager != null) {
        checkpointManager.createResources();
      }

      ChangelogStreamManager.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions);
    } catch (Exception e) {
      LOGGER.error("Exception while trying to getJobModel.", e);
      if (coordinatorListener != null) {
        coordinatorListener.onCoordinatorFailure(e);
      }
    }
    if (jobModel != null && jobModel.getContainers().containsKey(processorId)) {
      if (coordinatorListener != null) {
        coordinatorListener.onJobModelExpired();
        coordinatorListener.onNewJobModel(processorId, jobModel);
      }
    } else {
      stop();
    }
  }

  @Override
  public void stop() {
    // No-op
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    SystemAdmins systemAdmins = new SystemAdmins(config);
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 5000, SystemClock.instance());
    systemAdmins.start();
    try {
      String containerId = Integer.toString(config.getInt(JobConfig.PROCESSOR_ID()));
      GrouperContext grouperContext = new GrouperContext(ImmutableMap.of(String.valueOf(containerId), locationId), new HashMap<>(), new HashMap<>(), new HashMap<>());
      return JobModelManager.readJobModel(this.config, Collections.emptyMap(), streamMetadataCache, grouperContext);
    } finally {
      systemAdmins.stop();
    }
  }

  @Override
  public String getProcessorId() {
    return this.processorId;
  }

  private String createProcessorId(Config config) {
    // TODO: This check to be removed after 0.13+
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getProcessorId() != null) {
      return appConfig.getProcessorId();
    } else if (appConfig.getAppProcessorIdGeneratorClass() != null) {
      ProcessorIdGenerator idGenerator =
          Util.getObj(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(config);
    } else {
      throw new ConfigException(String
          .format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }
}
