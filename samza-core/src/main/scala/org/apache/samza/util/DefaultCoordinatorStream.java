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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SystemConfig;


public class DefaultCoordinatorStream implements CoordinatorStreamUtilFactory {
  @Override
  public Config buildCoordinatorStreamConfig(Config config) {
    Config jobConfig = new JobConfig(config);
    String jobName = ((JobConfig) jobConfig).getName().toString();
    String jobId = ((JobConfig) jobConfig).getJobId();

    // Build a map with just the system config and job.name/job.id. This is what's required to start the JobCoordinator.
    Map<String, String> map = config.subset(String.format(SystemConfig.SYSTEM_ID_PREFIX, ((JobConfig) jobConfig).getCoordinatorSystemName()), false);
    Map<String, String> addConfig = new HashMap<>();
    addConfig.put(JobConfig.JOB_NAME, jobName);
    addConfig.put(((JobConfig) jobConfig).JOB_ID, jobId);
    addConfig.put(JobConfig.JOB_COORDINATOR_SYSTEM, ((JobConfig) jobConfig).getCoordinatorSystemName());
    addConfig.put(((JobConfig) jobConfig).MONITOR_PARTITION_CHANGE_FREQUENCY_MS, String.valueOf(((JobConfig) jobConfig).getMonitorPartitionChangeFrequency()));

    addConfig.putAll(map);
    return  new MapConfig(addConfig);
  }
}
