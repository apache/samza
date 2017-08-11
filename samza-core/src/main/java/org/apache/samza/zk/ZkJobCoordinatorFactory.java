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
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkJobCoordinatorFactory implements JobCoordinatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinatorFactory.class);
  private static final String JOB_COORDINATOR_ZK_PATH_SUFFIX = "/JobCoordinatorData";

  /**
   * Method to instantiate an implementation of JobCoordinator
   *
   * @param config - configs relevant for the JobCoordinator TODO: Separate JC related configs into a "JobCoordinatorConfig"
   * @return An instance of IJobCoordinator
   */
  @Override
  public JobCoordinator getJobCoordinator(Config config) {
    MetricsRegistry metricsRegistry = new MetricsRegistryMap();
    ZkUtils zkUtils = getZkUtils(config, metricsRegistry);
    LOG.debug("Creating ZkJobCoordinator instance with config: {}.", config);
    return new ZkJobCoordinator(config, metricsRegistry, zkUtils);
  }

  private ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry) {
    ZkConfig zkConfig = new ZkConfig(config);
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder(new ApplicationConfig(config).getGlobalAppId() + JOB_COORDINATOR_ZK_PATH_SUFFIX);
    ZkClient zkClient = ZkCoordinationServiceFactory.createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
    return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), metricsRegistry);
  }
}
