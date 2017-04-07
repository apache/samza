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
package org.apache.samza.rest.proxy.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.rest.model.Partition;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.installation.InstallationFinder;
import org.apache.samza.rest.proxy.installation.InstallationRecord;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.storage.ChangelogPartitionManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * {@link TaskProxy} interface implementation for samza jobs running in yarn execution environment.
 * getTasks implementation reads the jobModel of the job specified by {@link JobInstance} from coordinator stream.
 */
public class SamzaTaskProxy implements TaskProxy {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaTaskProxy.class);

  private static final MetricsRegistryMap METRICS_REGISTRY = new MetricsRegistryMap();

  private static final String SOURCE = "SamzaTaskProxy";

  private final TaskResourceConfig taskResourceConfig;

  private final InstallationFinder installFinder;

  public SamzaTaskProxy(TaskResourceConfig taskResourceConfig, InstallationFinder installFinder) {
    this.taskResourceConfig = taskResourceConfig;
    this.installFinder = installFinder;
  }

  /**
   * Fetches the complete job model from the coordinator stream based upon the provided {@link JobInstance}
   * param, transforms it to a list of {@link Task} and returns it.
   * {@inheritDoc}
   */
  @Override
  public List<Task> getTasks(JobInstance jobInstance)
      throws IOException, InterruptedException {
    Preconditions.checkArgument(installFinder.isInstalled(jobInstance),
                                String.format("Invalid job instance : %s", jobInstance));
    JobModel jobModel = getJobModel(jobInstance);
    StorageConfig storageConfig = new StorageConfig(jobModel.getConfig());

    List<String> storeNames = JavaConverters.seqAsJavaListConverter(storageConfig.getStoreNames()).asJava();
    Map<String, String> containerLocality = jobModel.getAllContainerLocality();
    List<Task> tasks = new ArrayList<>();
    for (ContainerModel containerModel : jobModel.getContainers().values()) {
      String containerId = containerModel.getProcessorId();
      String host = containerLocality.get(containerId);
      for (TaskModel taskModel : containerModel.getTasks().values()) {
        String taskName = taskModel.getTaskName().getTaskName();
        List<Partition> partitions = taskModel.getSystemStreamPartitions()
                                              .stream()
                                              .map(Partition::new).collect(Collectors.toList());
        tasks.add(new Task(host, taskName, containerId, partitions, storeNames));
      }
    }
    return tasks;
  }

  /**
   * Builds coordinator system config for the {@param jobInstance}.
   * @param jobInstance the job instance to get the jobModel for.
   * @return the constructed coordinator system config.
   */
  private Config getCoordinatorSystemConfig(JobInstance jobInstance) {
    try {
      InstallationRecord record = installFinder.getAllInstalledJobs().get(jobInstance);
      ConfigFactory configFactory =  ClassLoaderHelper.fromClassName(taskResourceConfig.getJobConfigFactory());
      Config config = configFactory.getConfig(new URI(String.format("file://%s", record.getConfigFilePath())));
      Map<String, String> configMap = ImmutableMap.of(JobConfig.JOB_ID(), jobInstance.getJobId(),
                                                      JobConfig.JOB_NAME(), jobInstance.getJobName());
      return Util.buildCoordinatorStreamConfig(new MapConfig(ImmutableList.of(config, configMap)));
    } catch (Exception e) {
      LOG.error(String.format("Failed to get coordinator stream config for job : %s", jobInstance), e);
      throw new SamzaException(e);
    }
  }

  /**
   * Retrieves the jobModel from the jobCoordinator.
   * @param jobInstance the job instance (jobId, jobName).
   * @return the JobModel fetched from the coordinator stream.
   */
  protected JobModel getJobModel(JobInstance jobInstance) {
    CoordinatorStreamSystemConsumer coordinatorSystemConsumer = null;
    CoordinatorStreamSystemProducer coordinatorSystemProducer = null;
    try {
      CoordinatorStreamSystemFactory coordinatorStreamSystemFactory  = new CoordinatorStreamSystemFactory();
      Config coordinatorSystemConfig = getCoordinatorSystemConfig(jobInstance);
      LOG.info("Using config: {} to create coordinatorStream producer and consumer.", coordinatorSystemConfig);
      coordinatorSystemConsumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, METRICS_REGISTRY);
      coordinatorSystemProducer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(coordinatorSystemConfig, METRICS_REGISTRY);
      LOG.info("Registering coordinator system stream consumer.");
      coordinatorSystemConsumer.register();
      LOG.debug("Starting coordinator system stream consumer.");
      coordinatorSystemConsumer.start();
      LOG.debug("Bootstrapping coordinator system stream consumer.");
      coordinatorSystemConsumer.bootstrap();
      LOG.info("Registering coordinator system stream producer.");
      coordinatorSystemProducer.register(SOURCE);

      Config config = coordinatorSystemConsumer.getConfig();
      LOG.info("Got config from coordinatorSystemConsumer: {}.", config);
      ChangelogPartitionManager changelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, SOURCE);
      changelogManager.start();
      LocalityManager localityManager = new LocalityManager(coordinatorSystemProducer, coordinatorSystemConsumer);
      localityManager.start();
      return JobModelManager.readJobModel(config, changelogManager.readChangeLogPartitionMapping(), localityManager,
                                          new StreamMetadataCache(JobModelManager.getSystemAdmins(config), 0, SystemClock.instance()), null);
    } finally {
      if (coordinatorSystemConsumer != null) {
        coordinatorSystemConsumer.stop();
      }
      if (coordinatorSystemProducer != null) {
        coordinatorSystemProducer.stop();
      }
    }
  }
}
