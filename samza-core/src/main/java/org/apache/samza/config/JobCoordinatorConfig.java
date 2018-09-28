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
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.CoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.util.Util;
import org.apache.samza.zk.ZkCoordinationUtilsFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;

import java.util.Objects;

public class JobCoordinatorConfig extends MapConfig {
  public static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
  public final static String DEFAULT_COORDINATOR_UTILS_FACTORY = ZkJobCoordinatorFactory.class.getName();
  private static final String AZURE_COORDINATION_UTILS_FACTORY = "org.apache.samza.coordinator.AzureCoordinationUtilsFactory";
  private static final String AZURE_COORDINATOR_FACTORY = "org.apache.samza.coordinator.AzureJobCoordinatorFactory";

  public JobCoordinatorConfig(Config config) {
    super(config);
  }

  public String getJobCoordinationUtilsFactoryClassName() {
    String coordinatorFactory = get(JOB_COORDINATOR_FACTORY, DEFAULT_COORDINATOR_UTILS_FACTORY);

    String coordinationUtilsFactory;
    if (Objects.equals(coordinatorFactory, AZURE_COORDINATOR_FACTORY)) {
      coordinationUtilsFactory = AZURE_COORDINATION_UTILS_FACTORY;
    } else if (Objects.equals(coordinatorFactory, PassthroughJobCoordinatorFactory.class.getName())) {
      coordinationUtilsFactory = PassthroughCoordinationUtilsFactory.class.getName();
    } else {
      coordinationUtilsFactory = ZkCoordinationUtilsFactory.class.getName();
    }

    try {
      Class.forName(coordinationUtilsFactory);
    } catch (ClassNotFoundException e) {
      throw new SamzaException(
          "Failed to validate config value for " + JOB_COORDINATOR_FACTORY + " = " + coordinationUtilsFactory, e);
    }

    return coordinationUtilsFactory;
  }

  public CoordinationUtilsFactory getCoordinationUtilsFactory() {
    // load the class
    String coordinationUtilsFactoryClass = getJobCoordinationUtilsFactoryClassName();

    return Util.getObj(coordinationUtilsFactoryClass, CoordinationUtilsFactory.class);
  }

  public String getJobCoordinatorFactoryClassName() {
    String jobCoordinatorFactoryClassName = get(JOB_COORDINATOR_FACTORY);
    if (Strings.isNullOrEmpty(jobCoordinatorFactoryClassName)) {
      return ZkJobCoordinatorFactory.class.getName();
    } else {
      return jobCoordinatorFactoryClassName;
    }
  }
}
