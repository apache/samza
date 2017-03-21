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

package org.apache.samza.config;

import com.google.common.base.Strings;

public class JobCoordinatorConfig extends MapConfig {
  public static final String JOB_COORDINATOR_FACTORY = "job-coordinator.factory";
  public static final String JOB_COORDINATIOIN_SERVICE_FACTORY = "job-coordinationService.factory";

  public JobCoordinatorConfig(Config config) {
    super(config);
  }

  public String getJobCoordinatorFactoryClassName() {
    String jobCoordinatorFactoryClassName = get(JOB_COORDINATOR_FACTORY);
    if (Strings.isNullOrEmpty(jobCoordinatorFactoryClassName)) {
      throw new ConfigException(
          String.format("Missing config - %s. Cannot start StreamProcessor!", JOB_COORDINATOR_FACTORY));
    }

    return jobCoordinatorFactoryClassName;
  }

  public String getJobCoordinationServiceFactoryClassName() {
    String jobCooridanationFactoryClassName = get(JOB_COORDINATIOIN_SERVICE_FACTORY, "org.apache.samza.zk.ZkCoordinationServiceFactory");
    if (Strings.isNullOrEmpty(jobCooridanationFactoryClassName)) {
      throw new ConfigException(
          String.format("Missing config - %s. Cannot start coordiantion service!", JOB_COORDINATOR_FACTORY));
    }

    return jobCooridanationFactoryClassName;
  }
}
