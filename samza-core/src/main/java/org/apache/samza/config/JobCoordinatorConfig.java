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
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.zk.ZkCoordinationUtilsFactory;

public class JobCoordinatorConfig {
  public static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
  public static final String JOB_COORDINATION_UTILS_FACTORY = "job.coordination.utils.factory";
  public final static String DEFAULT_COORDINATION_UTILS_FACTORY = ZkCoordinationUtilsFactory.class.getName();

  private final Config config;

  public JobCoordinatorConfig(final Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
  }

  public String getJobCoordinationUtilsFactoryClassName() {
    String className = config.get(JOB_COORDINATION_UTILS_FACTORY, DEFAULT_COORDINATION_UTILS_FACTORY);

    if (Strings.isNullOrEmpty(className)) {
      throw new SamzaException("Empty config for " + JOB_COORDINATION_UTILS_FACTORY + " = " + className);
    }

    try {
      Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new SamzaException(
          "Failed to validate config value for " + JOB_COORDINATION_UTILS_FACTORY + " = " + className, e);
    }

    return className;
  }

  public CoordinationUtilsFactory getCoordinationUtilsFactory() {
    // load the class
    String coordinationUtilsFactoryClass = getJobCoordinationUtilsFactoryClassName();

    return ClassLoaderHelper.fromClassName(coordinationUtilsFactoryClass, CoordinationUtilsFactory.class);
  }

  public String getJobCoordinatorFactoryClassName() {
    String jobCoordinatorFactoryClassName = config.get(JOB_COORDINATOR_FACTORY);
    if (Strings.isNullOrEmpty(jobCoordinatorFactoryClassName)) {
      throw new ConfigException(
          String.format("Missing config - %s. Cannot start StreamProcessor!", JOB_COORDINATOR_FACTORY));
    }

    return jobCoordinatorFactoryClassName;
  }
}
