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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.processor.JobCoordinatorListener;
import org.apache.samza.processor.SamzaContainerController;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
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
public class StandaloneJobCoordinator implements JobCoordinator {
  private static final Logger log = LoggerFactory.getLogger(StandaloneJobCoordinator.class);
  private final String processorId;
  private final Config config;
  private final JobCoordinatorListener coordinatorListener;

  @VisibleForTesting
  StandaloneJobCoordinator(
      ProcessorIdGenerator processorIdGenerator,
      Config config,
      JobCoordinatorListener coordinatorListener) {
    this.processorId = processorIdGenerator.generateProcessorId(config);
    this.config = config;
    this.coordinatorListener = coordinatorListener;
  }

  public StandaloneJobCoordinator(String processorId, Config config, JobCoordinatorListener coordinatorListener) {
    this.config = config;
    this.processorId = processorId;
    this.coordinatorListener = coordinatorListener;
  }

  @Override
  public void start() {
    // No-op
    coordinatorListener.onNewJobModel(getJobModel());
  }

  @Override
  public void stop() {
    // No-op
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  @Override
  public JobModel getJobModel() {
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        log.error(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        throw new SamzaException(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
      }
      SystemFactory systemFactory = Util.<SystemFactory>getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());

    /** TODO:
     Locality Manager seems to be required in JC for reading locality info and grouping tasks intelligently and also,
     in SamzaContainer for writing locality info to the coordinator stream. This closely couples together
     TaskNameGrouper with the LocalityManager! Hence, groupers should be a property of the jobcoordinator
     (job.coordinator.task.grouper, instead of task.systemstreampartition.grouper)
     */
    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache, null);
  }
}
