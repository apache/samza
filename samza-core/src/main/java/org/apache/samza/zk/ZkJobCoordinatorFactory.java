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

package org.apache.samza.zk;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkJobCoordinatorFactory implements JobCoordinatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinatorFactory.class);
  private static final String JOB_COORDINATOR_ZK_PATH_FORMAT = "%s/%s-%s-%s-coordinationData";
  private static final String DEFAULT_JOB_NAME = "defaultJob";
  private static final String PROTOCOL_VERSION = "1.0";

  /**
   * Instantiates an {@link ZkJobCoordinator} using the {@link Config}.
   *
   * @param config zookeeper configurations required for instantiating {@link ZkJobCoordinator}
   * @return An instance of {@link ZkJobCoordinator}
   */
  @Override
  public JobCoordinator getJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry) {
    // TODO: Separate JC related configs into a "ZkJobCoordinatorConfig"
    String jobCoordinatorZkBasePath = getJobCoordinationZkPath(config);
    ZkUtils zkUtils = getZkUtils(config, metricsRegistry, jobCoordinatorZkBasePath);
    LOG.debug("Creating ZkJobCoordinator with config: {}.", config);
    return new ZkJobCoordinator(processorId, config, metricsRegistry, zkUtils);
  }

  private ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry, String coordinatorZkBasePath) {
    ZkConfig zkConfig = new ZkConfig(config);
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder(coordinatorZkBasePath);
    ZkClient zkClient = ZkCoordinationUtilsFactory
        .createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
    return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), zkConfig.getZkSessionTimeoutMs(), metricsRegistry);
  }

  public static String getJobCoordinationZkPath(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    String appId = new ApplicationConfig(config).getGlobalAppId();
    String jobName = jobConfig.getName().isDefined()
        ? jobConfig.getName().get()
        : DEFAULT_JOB_NAME;
    String jobId = jobConfig.getJobId();

    return String.format(JOB_COORDINATOR_ZK_PATH_FORMAT, appId, jobName, jobId, PROTOCOL_VERSION);
  }
}
