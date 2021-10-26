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

import java.util.Optional;
import com.google.common.base.Strings;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.CoordinationUtilsFactory;
import org.apache.samza.coordinator.lifecycle.NoOpJobRestartSignalFactory;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.zk.ZkCoordinationUtilsFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;

public class JobCoordinatorConfig extends MapConfig {
  public static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
  public final static String DEFAULT_COORDINATOR_FACTORY = ZkJobCoordinatorFactory.class.getName();
  public static final String JOB_RESTART_SIGNAL_FACTORY = "job.coordinator.restart.signal.factory";

  private static final String AZURE_COORDINATION_UTILS_FACTORY = "org.apache.samza.coordinator.AzureCoordinationUtilsFactory";
  private static final String AZURE_COORDINATOR_FACTORY = "org.apache.samza.coordinator.AzureJobCoordinatorFactory";
  private static final String DEFAULT_JOB_RESTART_SIGNAL_FACTORY = NoOpJobRestartSignalFactory.class.getName();

  public JobCoordinatorConfig(Config config) {
    super(config);
  }

  public String getJobCoordinationUtilsFactoryClassName() {
    String coordinatorFactory = get(JOB_COORDINATOR_FACTORY, DEFAULT_COORDINATOR_FACTORY);

    String coordinationUtilsFactory;
    if (AZURE_COORDINATOR_FACTORY.equals(coordinatorFactory)) {
      coordinationUtilsFactory = AZURE_COORDINATION_UTILS_FACTORY;
    } else if (PassthroughJobCoordinatorFactory.class.getName().equals(coordinatorFactory)) {
      coordinationUtilsFactory = PassthroughCoordinationUtilsFactory.class.getName();
    } else if (ZkJobCoordinatorFactory.class.getName().equals(coordinatorFactory)) {
      coordinationUtilsFactory = ZkCoordinationUtilsFactory.class.getName();
    } else {
      throw new SamzaException(String.format("Coordination factory: %s defined by the config: %s is invalid.", coordinatorFactory, JOB_COORDINATOR_FACTORY));
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

    return ReflectionUtil.getObj(coordinationUtilsFactoryClass, CoordinationUtilsFactory.class);
  }

  public String getJobCoordinatorFactoryClassName() {
    return getOptionalJobCoordinatorFactoryClassName()
        .filter(className -> !Strings.isNullOrEmpty(className))
        .orElse(ZkJobCoordinatorFactory.class.getName());
  }

  public Optional<String> getOptionalJobCoordinatorFactoryClassName() {
    return Optional.ofNullable(get(JOB_COORDINATOR_FACTORY));
  }

  public String getJobRestartSignalFactory() {
    return get(JOB_RESTART_SIGNAL_FACTORY, DEFAULT_JOB_RESTART_SIGNAL_FACTORY);
  }
}
