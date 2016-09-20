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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configs when Samza is used with a ClusterManager like Yarn or Mesos. Some of these configs were originally defined
 * in the yarn namespace. These will be moved to the "cluster-manager" namespace. For now, both configs will be honored
 * with the cluster-manager.* configs taking precedence. There will be a deprecated config warning when old configs are used.
 * Later, we'll enforce the new configs.
 */
public class ClusterManagerConfig extends MapConfig {

  private static final Logger log = LoggerFactory.getLogger(ClusterManagerConfig.class);


  private static final String CLUSTER_MANAGER_FACTORY = "samza.cluster-manager.factory";
  private static final String CLUSTER_MANAGER_FACTORY_DEFAULT = "org.apache.samza.job.yarn.YarnResourceManagerFactory";

  /**
   * Sleep interval for the allocator thread in milliseconds
   */
  private static final String ALLOCATOR_SLEEP_MS = "cluster-manager.allocator.sleep.ms";
  public static final String YARN_ALLOCATOR_SLEEP_MS = "yarn.allocator.sleep.ms";
  private static final int DEFAULT_ALLOCATOR_SLEEP_MS = 3600;

  /**
   * Number of milliseconds before a container request is considered to have to expired
   */
  public static final String CONTAINER_REQUEST_TIMEOUT_MS = "yarn.container.request.timeout.ms";
  public static final String CLUSTER_MANAGER_REQUEST_TIMEOUT_MS = "cluster-manager.container.request.timeout.ms";
  private static final int DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS = 5000;

  /**
   * Flag to indicate if host-affinity is enabled for the job or not
   */
  public static final String HOST_AFFINITY_ENABLED = "yarn.samza.host-affinity.enabled";
  public static final String CLUSTER_MANAGER_HOST_AFFINITY_ENABLED = "job.host-affinity.enabled";
  private static final boolean DEFAULT_HOST_AFFINITY_ENABLED = false;

  /**
   * Number of CPU cores to request from the cluster manager per container
   */
  public static final String CONTAINER_MAX_CPU_CORES = "yarn.container.cpu.cores";
  public static final String CLUSTER_MANAGER_MAX_CORES = "cluster-manager.container.cpu.cores";
  private static final int DEFAULT_CPU_CORES = 1;

  /**
   * Memory, in megabytes, to request from the cluster manager per container
   */
  public static final String CONTAINER_MAX_MEMORY_MB = "yarn.container.memory.mb";
  public static final String CLUSTER_MANAGER_MEMORY_MB = "cluster-manager.container.memory.mb";
  private static final int DEFAULT_CONTAINER_MEM = 1024;

  /**
   * Determines how frequently a container is allowed to fail before we give up and fail the job
   */
  public static final String CONTAINER_RETRY_WINDOW_MS = "yarn.container.retry.window.ms";
  public static final String CLUSTER_MANAGER_RETRY_WINDOW_MS = "cluster-manager.container.retry.window.ms";
  private static final int DEFAULT_CONTAINER_RETRY_WINDOW_MS = 300000;

  /**
   * Maximum number of times Samza tries to restart a failed container
   */
  public static final String CONTAINER_RETRY_COUNT = "yarn.container.retry.count";
  public static final String CLUSTER_MANAGER_CONTAINER_RETRY_COUNT = "cluster-manager.container.retry.count";
  private static final int DEFAULT_CONTAINER_RETRY_COUNT = 8;

  /**
   * Determines whether a JMX server should be started on the job coordinator
   * Default: true
   */
  public static final String AM_JMX_ENABLED = "yarn.am.jmx.enabled";
  public static final String CLUSTER_MANAGER_JMX_ENABLED = "cluster-manager.jobcoordinator.jmx.enabled";

  /**
   * The cluster managed job coordinator sleeps for a configurable time before checking again for termination.
   * The sleep interval of the cluster managed job coordinator.
   */
  public static final String CLUSTER_MANAGER_SLEEP_MS = "cluster-manager.jobcoordinator.sleep.interval.ms";
  private static final int DEFAULT_CLUSTER_MANAGER_SLEEP_MS = 1000;

  public ClusterManagerConfig(Config config) {
      super(config);
  }

  public int getAllocatorSleepTime() {
    if (containsKey(ALLOCATOR_SLEEP_MS)) {
      return getInt(ALLOCATOR_SLEEP_MS);
    } else if (containsKey(YARN_ALLOCATOR_SLEEP_MS)) {
      log.info("Configuration {} is deprecated. Please use {}", YARN_ALLOCATOR_SLEEP_MS, ALLOCATOR_SLEEP_MS);
      return getInt(YARN_ALLOCATOR_SLEEP_MS);
    } else {
      return DEFAULT_ALLOCATOR_SLEEP_MS;
    }
  }

  public int getNumCores() {
    if (containsKey(CLUSTER_MANAGER_MAX_CORES)) {
      return getInt(CLUSTER_MANAGER_MAX_CORES);
    } else if (containsKey(CONTAINER_MAX_CPU_CORES)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_MAX_CPU_CORES, CLUSTER_MANAGER_MAX_CORES);
      return getInt(CONTAINER_MAX_CPU_CORES);
    } else {
      return DEFAULT_CPU_CORES;
    }
  }

  public int getContainerMemoryMb() {
    if (containsKey(CLUSTER_MANAGER_MEMORY_MB)) {
      return getInt(CLUSTER_MANAGER_MEMORY_MB);
    } else if (containsKey(CONTAINER_MAX_MEMORY_MB)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_MAX_MEMORY_MB, CLUSTER_MANAGER_MEMORY_MB);
      return getInt(CONTAINER_MAX_MEMORY_MB);
    } else {
      return DEFAULT_CONTAINER_MEM;
    }
  }

  public boolean getHostAffinityEnabled() {
    if (containsKey(CLUSTER_MANAGER_HOST_AFFINITY_ENABLED)) {
      return getBoolean(CLUSTER_MANAGER_HOST_AFFINITY_ENABLED);
    } else if (containsKey(HOST_AFFINITY_ENABLED)) {
      log.info("Configuration {} is deprecated. Please use {}", HOST_AFFINITY_ENABLED, CLUSTER_MANAGER_HOST_AFFINITY_ENABLED);
      return getBoolean(HOST_AFFINITY_ENABLED);
    } else {
      return false;
    }
  }

  public int getContainerRequestTimeout() {
    if (containsKey(CLUSTER_MANAGER_REQUEST_TIMEOUT_MS)) {
      return getInt(CLUSTER_MANAGER_REQUEST_TIMEOUT_MS);
    } else if (containsKey(CONTAINER_REQUEST_TIMEOUT_MS)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_REQUEST_TIMEOUT_MS, CLUSTER_MANAGER_REQUEST_TIMEOUT_MS);
      return getInt(CONTAINER_REQUEST_TIMEOUT_MS);
    } else {
      return DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS;
    }
  }

  public int getContainerRetryCount() {
    if (containsKey(CLUSTER_MANAGER_CONTAINER_RETRY_COUNT))
      return getInt(CLUSTER_MANAGER_CONTAINER_RETRY_COUNT);
    else if (containsKey(CONTAINER_RETRY_COUNT)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_RETRY_COUNT, CLUSTER_MANAGER_CONTAINER_RETRY_COUNT);
      return getInt(CONTAINER_RETRY_COUNT);
    } else {
      return DEFAULT_CONTAINER_RETRY_COUNT;
    }
  }

  public int getContainerRetryWindowMs() {
    if (containsKey(CLUSTER_MANAGER_RETRY_WINDOW_MS)) {
      return getInt(CLUSTER_MANAGER_RETRY_WINDOW_MS);
    } else if (containsKey(CONTAINER_RETRY_WINDOW_MS)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_RETRY_WINDOW_MS, CLUSTER_MANAGER_RETRY_WINDOW_MS);
      return getInt(CONTAINER_RETRY_WINDOW_MS);
    } else {
      return DEFAULT_CONTAINER_RETRY_WINDOW_MS;
    }
  }


  public int getJobCoordinatorSleepInterval() {
    return getInt(CLUSTER_MANAGER_SLEEP_MS, DEFAULT_CLUSTER_MANAGER_SLEEP_MS);
  }

  public String getContainerManagerClass() {
    return get(CLUSTER_MANAGER_FACTORY, CLUSTER_MANAGER_FACTORY_DEFAULT);
  }

  public boolean getJmxEnabled() {
    if (containsKey(CLUSTER_MANAGER_JMX_ENABLED)) {
      return getBoolean(CLUSTER_MANAGER_JMX_ENABLED);
    } else if (containsKey(AM_JMX_ENABLED)) {
      log.info("Configuration {} is deprecated. Please use {}", AM_JMX_ENABLED, CLUSTER_MANAGER_JMX_ENABLED);
      return getBoolean(AM_JMX_ENABLED);
    } else {
      return true;
    }
  }
}
