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
import java.util.Collections;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.runtime.LocationIdProvider;
import org.apache.samza.runtime.LocationIdProviderFactory;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  public PassthroughJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry) {
    this.processorId = processorId;
    this.config = config;
    LocationIdProviderFactory locationIdProviderFactory =
        ReflectionUtil.getObj(new JobConfig(config).getLocationIdProviderFactory(), LocationIdProviderFactory.class);
    LocationIdProvider locationIdProvider = locationIdProviderFactory.getLocationIdProvider(config);
    this.locationId = locationIdProvider.getLocationId();
  }

  @Override
  public void start() {
    // No-op
    JobModel jobModel = null;
    try {
      jobModel = getJobModel();
      // TODO metrics registry has been null here for a while; is it safe?
      MetadataResourceUtil metadataResourceUtil = new MetadataResourceUtil(jobModel, null, config);
      metadataResourceUtil.createResources();
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
      LOGGER.info("JobModel: {} does not contain processorId: {}. Stopping the JobCoordinator", jobModel, processorId);
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
    SystemAdmins systemAdmins = new SystemAdmins(config, this.getClass().getSimpleName());
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 5000, SystemClock.instance());
    systemAdmins.start();
    try {
      String containerId = Integer.toString(config.getInt(JobConfig.PROCESSOR_ID));
      GrouperMetadata grouperMetadata =
          new GrouperMetadataImpl(ImmutableMap.of(String.valueOf(containerId), locationId), Collections.emptyMap(),
              Collections.emptyMap(), Collections.emptyMap());
      return JobModelManager.readJobModel(this.config, Collections.emptyMap(), streamMetadataCache, grouperMetadata);
    } finally {
      systemAdmins.stop();
    }
  }

  @Override
  public String getProcessorId() {
    return this.processorId;
  }
}
