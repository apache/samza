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
package org.apache.samza.coordinator.staticresource;

import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.container.grouper.task.TaskPartitionAssignmentManager;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.coordinator.JobModelCalculator;
import org.apache.samza.coordinator.JobModelHelper;
import org.apache.samza.coordinator.StreamPartitionCountMonitorFactory;
import org.apache.samza.coordinator.StreamRegexMonitorFactory;
import org.apache.samza.coordinator.communication.CoordinatorCommunication;
import org.apache.samza.coordinator.communication.CoordinatorCommunicationContext;
import org.apache.samza.coordinator.communication.HttpCoordinatorToWorkerCommunicationFactory;
import org.apache.samza.coordinator.communication.JobInfoServingContext;
import org.apache.samza.coordinator.lifecycle.JobRestartSignal;
import org.apache.samza.coordinator.lifecycle.JobRestartSignalFactory;
import org.apache.samza.coordinator.lifecycle.JobRestartSignalFactoryContext;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetJobCoordinatorMetadataMessage;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskPartitionMapping;
import org.apache.samza.environment.EnvironmentVariables;
import org.apache.samza.job.metadata.JobCoordinatorMetadataManager;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;


public class StaticResourceJobCoordinatorFactory implements JobCoordinatorFactory {
  @Override
  public JobCoordinator getJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry,
      MetadataStore metadataStore) {
    JobInfoServingContext jobModelServingContext = new JobInfoServingContext();
    JobConfig jobConfig = new JobConfig(config);
    CoordinatorCommunicationContext context =
        new CoordinatorCommunicationContext(jobModelServingContext, config, metricsRegistry);
    CoordinatorCommunication coordinatorCommunication =
        new HttpCoordinatorToWorkerCommunicationFactory().coordinatorCommunication(context);
    JobCoordinatorMetadataManager jobCoordinatorMetadataManager = new JobCoordinatorMetadataManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetJobCoordinatorMetadataMessage.TYPE),
        JobCoordinatorMetadataManager.ClusterType.NON_YARN, metricsRegistry);
    ChangelogStreamManager changelogStreamManager =
        new ChangelogStreamManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
    JobRestartSignal jobRestartSignal =
        ReflectionUtil.getObj(new JobCoordinatorConfig(config).getJobRestartSignalFactory(),
            JobRestartSignalFactory.class).build(new JobRestartSignalFactoryContext(config));
    Optional<StartpointManager> startpointManager =
        jobConfig.getStartpointEnabled() ? Optional.of(new StartpointManager(metadataStore)) : Optional.empty();
    SystemAdmins systemAdmins = new SystemAdmins(config, StaticResourceJobCoordinator.class.getSimpleName());
    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    JobModelHelper jobModelHelper = buildJobModelHelper(metadataStore, streamMetadataCache);
    StreamPartitionCountMonitorFactory streamPartitionCountMonitorFactory =
        new StreamPartitionCountMonitorFactory(streamMetadataCache, metricsRegistry);
    StreamRegexMonitorFactory streamRegexMonitorFactory =
        new StreamRegexMonitorFactory(streamMetadataCache, metricsRegistry);
    Optional<String> executionEnvContainerId =
        Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID));
    Optional<String> samzaEpochId = Optional.ofNullable(System.getenv(EnvironmentVariables.SAMZA_EPOCH_ID));
    return new StaticResourceJobCoordinator(processorId, jobModelHelper, jobModelServingContext,
        coordinatorCommunication, jobCoordinatorMetadataManager, streamPartitionCountMonitorFactory,
        streamRegexMonitorFactory, startpointManager, changelogStreamManager, jobRestartSignal, metricsRegistry,
        systemAdmins, executionEnvContainerId, samzaEpochId, config);
  }

  private static JobModelHelper buildJobModelHelper(MetadataStore metadataStore,
      StreamMetadataCache streamMetadataCache) {
    LocalityManager localityManager =
        new LocalityManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetContainerHostMapping.TYPE));
    TaskAssignmentManager taskAssignmentManager =
        new TaskAssignmentManager(new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskContainerMapping.TYPE),
            new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskModeMapping.TYPE));
    TaskPartitionAssignmentManager taskPartitionAssignmentManager = new TaskPartitionAssignmentManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetTaskPartitionMapping.TYPE));
    return new JobModelHelper(localityManager, taskAssignmentManager, taskPartitionAssignmentManager,
        streamMetadataCache, JobModelCalculator.INSTANCE);
  }
}
