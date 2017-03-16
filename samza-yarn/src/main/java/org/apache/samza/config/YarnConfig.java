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

import org.apache.samza.SamzaException;

public class YarnConfig extends MapConfig {
  /**
   * (Required) URL from which the job package can be downloaded
   */
  public static final String PACKAGE_PATH = "yarn.package.path";

  /**
   * The prefix for defining any optional yarn localizer resource
   * The key will be localizer.resource.&lt;resourceVisibility&gt;.&lt;resourceType&gt;.&lt;resourceName&gt;
   * The value corresponding to the key will be the URI.
   */
  public static final String LOCALIZER_RESOURCE_PREFIX = "yarn.localizer.resource.";

  // Configs related to each yarn container
  /**
   * Memory, in megabytes, to request from YARN per container
   */
  public static final String CONTAINER_MAX_MEMORY_MB = "yarn.container.memory.mb";
  private static final int DEFAULT_CONTAINER_MEM = 1024;

  /**
   * Name of YARN queue to run jobs on
   */
  public static final String QUEUE_NAME = "yarn.queue";

  /**
   * Number of CPU cores to request from YARN per container
   */
  public static final String CONTAINER_MAX_CPU_CORES = "yarn.container.cpu.cores";
  private static final int DEFAULT_CPU_CORES = 1;

  /**
   * Label to request from YARN for containers
   */
  public static final String CONTAINER_LABEL = "yarn.container.label";

  /**
   * Maximum number of times the AM tries to restart a failed container
   */
  public static final String CONTAINER_RETRY_COUNT = "yarn.container.retry.count";
  private static final int DEFAULT_CONTAINER_RETRY_COUNT = 8;

  /**
   * Determines how frequently a container is allowed to fail before we give up and fail the job
   */
  public static final String CONTAINER_RETRY_WINDOW_MS = "yarn.container.retry.window.ms";
  private static final int DEFAULT_CONTAINER_RETRY_WINDOW_MS = 300000;

  // Configs related to the Samza Application Master (AM)
  /**
   * (Optional) JVM options to include in the command line when executing the AM
   */
  public static final String AM_JVM_OPTIONS = "yarn.am.opts";

  /**
   * Determines whether a JMX server should be started on the AM
   * Default: true
   */
  public static final String AM_JMX_ENABLED = "yarn.am.jmx.enabled";

  /**
   * Memory, in megabytes, to request from YARN for running the AM
   */
  public static final String AM_CONTAINER_MAX_MEMORY_MB = "yarn.am.container.memory.mb";
  private static final int DEFAULT_AM_CONTAINER_MAX_MEMORY_MB = 1024;

  /**
   * Label to request from YARN for running the AM
   */
  public static final String AM_CONTAINER_LABEL = "yarn.am.container.label";

  /**
   * Number of CPU cores to request from YARN for running the AM
   */
  public static final String AM_CONTAINER_MAX_CPU_CORES = "yarn.am.container.cpu.cores";
  private static final int DEFAULT_AM_CPU_CORES = 1;

  /**
   * Determines the interval for the Heartbeat between the AM and the Yarn RM
   */
  public static final String AM_POLL_INTERVAL_MS = "yarn.am.poll.interval.ms";
  private static final int DEFAULT_POLL_INTERVAL_MS = 1000;

  /**
   * (Optional) JAVA_HOME path for Samza AM
   */
  public static final String AM_JAVA_HOME = "yarn.am.java.home";

  // Configs related to the ContainerAllocator thread
  /**
   * Sleep interval for the allocator thread in milliseconds
   */
  public static final String ALLOCATOR_SLEEP_MS = "yarn.allocator.sleep.ms";
  private static final int DEFAULT_ALLOCATOR_SLEEP_MS = 3600;
  /**
   * Number of milliseconds before a container request is considered to have to expired
   */
  public static final String CONTAINER_REQUEST_TIMEOUT_MS = "yarn.container.request.timeout.ms";
  private static final int DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS = 5000;

  /**
   * Flag to indicate if host-affinity is enabled for the job or not
   */
  public static final String HOST_AFFINITY_ENABLED = "yarn.samza.host-affinity.enabled";
  private static final boolean DEFAULT_HOST_AFFINITY_ENABLED = false;

  /**
   * Principal used to log in on a Kerberized secure cluster
   */
  public static final String YARN_KERBEROS_PRINCIPAL = "yarn.kerberos.principal";

  /**
   * Key tab used to log in on a Kerberized secure cluster
   */
  public static final String YARN_KERBEROS_KEYTAB = "yarn.kerberos.keytab";

  /**
   * Interval in seconds to renew a delegation token in Kerberized secure cluster
   */
  public static final String YARN_TOKEN_RENEWAL_INTERVAL_SECONDS = "yarn.token.renewal.interval.seconds";
  private static final long DEFAULT_YARN_TOKEN_RENEWAL_INTERVAL_SECONDS = 24 * 3600;

  /**
   * The location on HDFS to store the credentials file
   */
  public static final String YARN_CREDENTIALS_FILE = "yarn.credentials.file";

  /**
   * The staging directory on HDFS for the job
   */
  public static final String YARN_JOB_STAGING_DIRECTORY = "yarn.job.staging.directory";

  public YarnConfig(Config config) {
    super(config);
  }

  public int getContainerRetryCount() {
    return getInt(CONTAINER_RETRY_COUNT, DEFAULT_CONTAINER_RETRY_COUNT);
  }

  public int getContainerRetryWindowMs() {
    return getInt(CONTAINER_RETRY_WINDOW_MS, DEFAULT_CONTAINER_RETRY_WINDOW_MS);
  }

  public int getAMPollIntervalMs() {
    return getInt(AM_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
  }

  public int getContainerMaxMemoryMb() {
    return getInt(CONTAINER_MAX_MEMORY_MB, DEFAULT_CONTAINER_MEM);
  }

  public int getContainerMaxCpuCores() {
    return getInt(CONTAINER_MAX_CPU_CORES, DEFAULT_CPU_CORES);
  }

  public String getContainerLabel() {
    return get(CONTAINER_LABEL, null);
  }

  public boolean getJmxServerEnabled() {
    return getBoolean(AM_JMX_ENABLED, true);
  }

  public String getPackagePath() {
    String packagePath = get(PACKAGE_PATH);
    if (packagePath == null) {
      throw new SamzaException("No YARN package path defined in config.");
    }
    return packagePath;
  }

  public Config getLocalizerResourceConfigs() {
    Config localizerResourceConfigs = subset(LOCALIZER_RESOURCE_PREFIX, false); // do not strip off the prefix
    if (localizerResourceConfigs == null || localizerResourceConfigs.isEmpty()) {
      throw new SamzaException("No Samza app localizer resource path defined in config.");
    }
    return localizerResourceConfigs;
  }


  public int getAMContainerMaxMemoryMb() {
    return getInt(AM_CONTAINER_MAX_MEMORY_MB, DEFAULT_AM_CONTAINER_MAX_MEMORY_MB);
  }

  public String getAMContainerLabel() {
    return get(AM_CONTAINER_LABEL, null);
  }

  public int getAMContainerMaxCpuCores() {
    return getInt(AM_CONTAINER_MAX_CPU_CORES, DEFAULT_AM_CPU_CORES);
  }

  public String getAmOpts() {
    return get(AM_JVM_OPTIONS, "");
  }

  public String getQueueName() {
    return get(QUEUE_NAME, null);
  }

  public String getAMJavaHome() {
    return get(AM_JAVA_HOME, null);
  }

  public int getAllocatorSleepTime() {
    return getInt(ALLOCATOR_SLEEP_MS, DEFAULT_ALLOCATOR_SLEEP_MS);
  }

  public int getContainerRequestTimeout() {
    return getInt(CONTAINER_REQUEST_TIMEOUT_MS, DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS);
  }

  public boolean getHostAffinityEnabled() {
    return getBoolean(HOST_AFFINITY_ENABLED, DEFAULT_HOST_AFFINITY_ENABLED);
  }

  public String getYarnKerberosPrincipal() {
    return get(YARN_KERBEROS_PRINCIPAL, null);
  }

  public String getYarnKerberosKeytab() {
    return get(YARN_KERBEROS_KEYTAB, null);
  }

  public long getYarnTokenRenewalIntervalSeconds() {
    return getLong(YARN_TOKEN_RENEWAL_INTERVAL_SECONDS, DEFAULT_YARN_TOKEN_RENEWAL_INTERVAL_SECONDS);
  }

  public String getYarnCredentialsFile() {
    return get(YARN_CREDENTIALS_FILE, null);
  }

  public String getYarnJobStagingDirectory() {
    return get(YARN_JOB_STAGING_DIRECTORY, null);
  }
}
