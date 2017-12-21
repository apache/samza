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

public class YarnConfig {
  /**
   * (Required) URL from which the job package can be downloaded
   */
  public static final String PACKAGE_PATH = "yarn.package.path";

  /**
   * Name of YARN queue to run jobs on
   */
  public static final String QUEUE_NAME = "yarn.queue";

  /**
   * Label to request from YARN for containers
   */
  public static final String CONTAINER_LABEL = "yarn.container.label";

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

  private final Config config;

  public YarnConfig(Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
  }

  public int getAMPollIntervalMs() {
    return config.getInt(AM_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
  }

  public String getContainerLabel() {
    return config.get(CONTAINER_LABEL, null);
  }

  public boolean getJmxServerEnabled() {
    return config.getBoolean(AM_JMX_ENABLED, true);
  }

  public String getPackagePath() {
    String packagePath = config.get(PACKAGE_PATH);
    if (packagePath == null) {
      throw new SamzaException("No YARN package path defined in config.");
    }
    return packagePath;
  }

  public int getAMContainerMaxMemoryMb() {
    return config.getInt(AM_CONTAINER_MAX_MEMORY_MB, DEFAULT_AM_CONTAINER_MAX_MEMORY_MB);
  }

  public String getAMContainerLabel() {
    return config.get(AM_CONTAINER_LABEL, null);
  }

  public int getAMContainerMaxCpuCores() {
    return config.getInt(AM_CONTAINER_MAX_CPU_CORES, DEFAULT_AM_CPU_CORES);
  }

  public String getAmOpts() {
    return config.get(AM_JVM_OPTIONS, "");
  }

  public String getQueueName() {
    return config.get(QUEUE_NAME, null);
  }

  public String getAMJavaHome() {
    return config.get(AM_JAVA_HOME, null);
  }

  public int getAllocatorSleepTime() {
    return config.getInt(ALLOCATOR_SLEEP_MS, DEFAULT_ALLOCATOR_SLEEP_MS);
  }

  public int getContainerRequestTimeout() {
    return config.getInt(CONTAINER_REQUEST_TIMEOUT_MS, DEFAULT_CONTAINER_REQUEST_TIMEOUT_MS);
  }

  public boolean getHostAffinityEnabled() {
    return config.getBoolean(HOST_AFFINITY_ENABLED, DEFAULT_HOST_AFFINITY_ENABLED);
  }

  public String getYarnKerberosPrincipal() {
    return config.get(YARN_KERBEROS_PRINCIPAL, null);
  }

  public String getYarnKerberosKeytab() {
    return config.get(YARN_KERBEROS_KEYTAB, null);
  }

  public long getYarnTokenRenewalIntervalSeconds() {
    return config.getLong(YARN_TOKEN_RENEWAL_INTERVAL_SECONDS, DEFAULT_YARN_TOKEN_RENEWAL_INTERVAL_SECONDS);
  }

  public String getYarnCredentialsFile() {
    return config.get(YARN_CREDENTIALS_FILE, null);
  }

  public String getYarnJobStagingDirectory() {
    return config.get(YARN_JOB_STAGING_DIRECTORY, null);
  }
}
