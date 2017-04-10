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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.processor.SamzaContainerController;

public class ZkJobCoordinatorFactory implements JobCoordinatorFactory {
  /**
   * Method to instantiate an implementation of JobCoordinator
   *
   * @param config  Configs relevant for the JobCoordinator TODO: Separate JC related configs into a "JobCoordinatorConfig"
   * @return An instance of IJobCoordinator
   */
  @Override
  public JobCoordinator getJobCoordinator(Config config, SamzaContainerController containerController) {
    JobConfig jobConfig = new JobConfig(config);
    String groupName = String.format("%s-%s", jobConfig.getName().get(), jobConfig.getJobId().get());
    ZkConfig zkConfig = new ZkConfig(config);
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();
    ZkClient zkClient = new ZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());

    return new ZkJobCoordinator(
        groupName,
        config,
        debounceTimer,
        new ZkUtils(
            new ZkKeyBuilder(groupName),
            zkClient,
            zkConfig.getZkConnectionTimeoutMs()
            ),
        containerController);
  }
}
