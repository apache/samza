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

import java.time.Duration;
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

  private static final String FAULT_DOMAIN_MANAGER_FACTORY = "cluster-manager.fault-domain-manager.factory";
  private static final String FAULT_DOMAIN_MANAGER_FACTORY_DEFAULT = "org.apache.samza.job.yarn.YarnFaultDomainManagerFactory";

  /**
   * Determines whether standby allocation is fault domain aware or not.
   */
  public static final String FAULT_DOMAIN_AWARE_STANDBY_ENABLED = "cluster-manager.fault-domain-aware.standby.enabled";
  public static final boolean FAULT_DOMAIN_AWARE_STANDBY_ENABLED_DEFAULT = false;

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
   * NOTE: This field is deprecated.
   */
  public static final String HOST_AFFINITY_ENABLED = "yarn.samza.host-affinity.enabled";

  /**
   * Flag to indicate if host-affinity is enabled for the job or not
   */
  public static final String JOB_HOST_AFFINITY_ENABLED = "job.host-affinity.enabled";

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
  public static final int DEFAULT_CONTAINER_RETRY_COUNT = 8;

  /**
   * Determines if a job should fail after any container has exhausted all its retries.
   */
  public static final String CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES = "cluster-manager.container.fail.job.after.retries";
  public static final boolean DEFAULT_CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES = true;

  /**
   * Maximum delay in milliseconds for the last container retry
   */
  public static final String CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS =
      "cluster-manager.container.preferred-host.last.retry.delay.ms";
  private static final long CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_RETRY_DELAY_CLOCK_SKEW_DELTA =
      Duration.ofSeconds(1).toMillis();
  private static final long DEFAULT_CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS =
      Duration.ofMinutes(6).toMillis() + CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_RETRY_DELAY_CLOCK_SKEW_DELTA;

  /**
   * The cluster managed job coordinator sleeps for a configurable time before checking again for termination.
   * The sleep interval of the cluster managed job coordinator.
   */
  public static final String CLUSTER_MANAGER_SLEEP_MS = "cluster-manager.jobcoordinator.sleep.interval.ms";
  private static final int DEFAULT_CLUSTER_MANAGER_SLEEP_MS = 1000;

  /**
   * Determines whether a JMX server should be started on JobCoordinator and SamzaContainer
   * Default: true
   */
  private static final String JOB_JMX_ENABLED = "job.jmx.enabled";

  /**
   * Determines whether a JMX server should be started on the job coordinator
   * Default: true
   *
   * @deprecated use {@code JOB_JMX_ENABLED} instead
   */
  private static final String AM_JMX_ENABLED = "yarn.am.jmx.enabled";
  private static final String CLUSTER_MANAGER_JMX_ENABLED = "cluster-manager.jobcoordinator.jmx.enabled";

  /**
   * Use this to configure a static port for the job coordinator url for a Samza job. This url is used to provide
   * information such as job model and locality.
   * If the value is set to 0, then the port will be dynamically allocated from the available free ports on the node.
   * The default value of this config is 0.
   *
   * Be careful when using this configuration. If the configured port is already in use on the node, then the job
   * coordinator will fail to start.
   *
   * This configuration is experimental, and it might be removed in a future release.
   */
  private static final String JOB_COORDINATOR_URL_PORT = "cluster-manager.jobcoordinator.url.port";
  private static final int DEFAULT_JOB_COORDINATOR_URL_PORT = 0;

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

  /**
   * Return the value of CLUSTER_MANAGER_MAX_CORES or CONTAINER_MAX_CPU_CORES (in that order) if autosizing is not enabled,
   * otherwise returns the value of JOB_AUTOSIZING_CONTAINER_MAX_CORES.
   * @return
   */
  public int getNumCores() {
    if (new JobConfig(this).getAutosizingEnabled() && containsKey(JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_CORES)) {
      return getInt(JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_CORES);
    } else if (containsKey(CLUSTER_MANAGER_MAX_CORES)) {
      return getInt(CLUSTER_MANAGER_MAX_CORES);
    } else if (containsKey(CONTAINER_MAX_CPU_CORES)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_MAX_CPU_CORES, CLUSTER_MANAGER_MAX_CORES);
      return getInt(CONTAINER_MAX_CPU_CORES);
    } else {
      return DEFAULT_CPU_CORES;
    }
  }

  /**
   * Return the value of CLUSTER_MANAGER_MEMORY_MB or CONTAINER_MAX_MEMORY_MB (in that order) if autosizing is not enabled,
   * otherwise returns the value of JOB_AUTOSIZING_CONTAINER_MEMORY_MB.
   * @return
   */
  public int getContainerMemoryMb() {
    if (new JobConfig(this).getAutosizingEnabled() && containsKey(JobConfig.JOB_AUTOSIZING_CONTAINER_MEMORY_MB)) {
      return getInt(JobConfig.JOB_AUTOSIZING_CONTAINER_MEMORY_MB);
    } else if (containsKey(CLUSTER_MANAGER_MEMORY_MB)) {
      return getInt(CLUSTER_MANAGER_MEMORY_MB);
    } else if (containsKey(CONTAINER_MAX_MEMORY_MB)) {
      log.info("Configuration {} is deprecated. Please use {}", CONTAINER_MAX_MEMORY_MB, CLUSTER_MANAGER_MEMORY_MB);
      return getInt(CONTAINER_MAX_MEMORY_MB);
    } else {
      return DEFAULT_CONTAINER_MEM;
    }
  }

  public boolean getHostAffinityEnabled() {
    if (containsKey(JOB_HOST_AFFINITY_ENABLED)) {
      return getBoolean(JOB_HOST_AFFINITY_ENABLED);
    } else if (containsKey(HOST_AFFINITY_ENABLED)) {
      log.warn("Configuration {} is deprecated. Please use {}", HOST_AFFINITY_ENABLED, JOB_HOST_AFFINITY_ENABLED);
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

  /**
   * The value of {@link ClusterManagerConfig#CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES} that determines if the
   * job will fail if any container has exhausted all its retries and each retry is within the {@link ClusterManagerConfig#CLUSTER_MANAGER_RETRY_WINDOW_MS}.
   * @return true if the job should fail after any container has exhausted all its retries; otherwise, false.
   */
  public boolean shouldFailJobAfterContainerRetries() {
    return getBoolean(CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES,
        DEFAULT_CLUSTER_MANAGER_CONTAINER_FAIL_JOB_AFTER_RETRIES);
  }

  public long getContainerPreferredHostLastRetryDelayMs() {
    if (containsKey(CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS)) {
      return getLong(CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS) + CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_RETRY_DELAY_CLOCK_SKEW_DELTA;
    } else {
      return DEFAULT_CLUSTER_MANAGER_CONTAINER_PREFERRED_HOST_LAST_RETRY_DELAY_MS;
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

  public String getFaultDomainManagerClass() {
    return get(FAULT_DOMAIN_MANAGER_FACTORY, FAULT_DOMAIN_MANAGER_FACTORY_DEFAULT);
  }

  public boolean getFaultDomainAwareStandbyEnabled() {
    return getBoolean(FAULT_DOMAIN_AWARE_STANDBY_ENABLED, FAULT_DOMAIN_AWARE_STANDBY_ENABLED_DEFAULT);
  }

  public boolean getJmxEnabledOnJobCoordinator() {
    if (containsKey(CLUSTER_MANAGER_JMX_ENABLED)) {
      log.warn("Configuration {} is deprecated. Please use {}", CLUSTER_MANAGER_JMX_ENABLED, JOB_JMX_ENABLED);
      return getBoolean(CLUSTER_MANAGER_JMX_ENABLED);
    } else if (containsKey(AM_JMX_ENABLED)) {
      log.warn("Configuration {} is deprecated. Please use {}", AM_JMX_ENABLED, JOB_JMX_ENABLED);
      return getBoolean(AM_JMX_ENABLED);
    } else if (containsKey(JOB_JMX_ENABLED)) {
      return getBoolean(JOB_JMX_ENABLED);
    } else {
      return true;
    }
  }

  public int getCoordinatorUrlPort() {
    return getInt(JOB_COORDINATOR_URL_PORT, DEFAULT_JOB_COORDINATOR_URL_PORT);
  }
}
