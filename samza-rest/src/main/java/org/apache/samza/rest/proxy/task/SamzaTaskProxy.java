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
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.installation.InstallationFinder;
import org.apache.samza.rest.proxy.installation.InstallationRecord;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.util.ClassLoaderHelper;
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
  public List<Task> getTasks(JobInstance jobInstance) throws IOException, InterruptedException {
    Preconditions.checkArgument(installFinder.isInstalled(jobInstance),
                                String.format("Invalid job instance : %s", jobInstance));
    CoordinatorStreamSystemConsumer coordinatorStreamSystemConsumer = null;
    try {
      coordinatorStreamSystemConsumer = initializeCoordinatorStreamConsumer(jobInstance);
      return readTasksFromCoordinatorStream(coordinatorStreamSystemConsumer);
    } finally {
      if (coordinatorStreamSystemConsumer != null) {
        coordinatorStreamSystemConsumer.stop();
      }
    }
  }

  /**
   * Initialize {@link CoordinatorStreamSystemConsumer} based upon {@link JobInstance} parameter.
   * @param jobInstance the job instance to get CoordinatorStreamSystemConsumer for.
   * @return built and initialized CoordinatorStreamSystemConsumer.
   */
  protected CoordinatorStreamSystemConsumer initializeCoordinatorStreamConsumer(JobInstance jobInstance) {
    CoordinatorStreamSystemFactory coordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory();
    Config coordinatorSystemConfig = getCoordinatorSystemConfig(jobInstance);
    LOG.debug("Using config: {} to create coordinatorStream consumer.", coordinatorSystemConfig);
    CoordinatorStreamSystemConsumer consumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, METRICS_REGISTRY);
    LOG.debug("Registering coordinator system stream consumer.");
    consumer.register();
    LOG.debug("Starting coordinator system stream consumer.");
    consumer.start();
    LOG.debug("Bootstrapping coordinator system stream consumer.");
    consumer.bootstrap();
    return consumer;
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
   * Builds list of {@link Task} from job model in coordinator stream.
   * @param consumer system consumer associated with a job's coordinator stream.
   * @return list of {@link Task} constructed from job model in coordinator stream.
   */
  protected List<Task> readTasksFromCoordinatorStream(CoordinatorStreamSystemConsumer consumer) {
    LocalityManager localityManager = new LocalityManager(null, consumer);
    Map<String, Map<String, String>> containerIdToHostMapping = localityManager.readContainerLocality();
    Map<String, String> taskNameToContainerIdMapping = localityManager.getTaskAssignmentManager().readTaskAssignment();
    StorageConfig storageConfig = new StorageConfig(consumer.getConfig());
    List<String> storeNames = JavaConverters.seqAsJavaListConverter(storageConfig.getStoreNames()).asJava();
    return taskNameToContainerIdMapping.entrySet()
                                       .stream()
                                       .map(entry -> {
        String hostName = containerIdToHostMapping.get(entry.getValue()).get(SetContainerHostMapping.HOST_KEY);
        return new Task(hostName, entry.getKey(), entry.getValue(), new ArrayList<>(), storeNames);
                                       }).collect(Collectors.toList());
  }
}
