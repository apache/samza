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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.samza.SamzaException;

public class YarnConfig extends MapConfig {
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

  /**
   * For secured YARN cluster only.
   * The 'viewing' acl of the YARN application. This controls who can view the application,
   * for example, application status, logs.
   * {@link org.apache.hadoop.yarn.api.records.ApplicationAccessType} for more details
   */
  public static final String YARN_APPLICATION_VIEW_ACL = "yarn.job.view.acl";

  /**
   * For secured YARN cluster only.
   * The 'modify' acl of the YARN application. This controls who can modify the application,
   * for example, killing the job.
   * {@link org.apache.hadoop.yarn.api.records.ApplicationAccessType} for more details
   */
  public static final String YARN_APPLICATION_MODIFY_ACL = "yarn.job.modify.acl";

  public YarnConfig(Config config) {
    super(config);
  }

  public int getAMPollIntervalMs() {
    return getInt(AM_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
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

  public String getYarnApplicationViewAcl() {
    return get(YARN_APPLICATION_VIEW_ACL, null);
  }

  public String getYarnApplicationModifyAcl() {
    return get(YARN_APPLICATION_MODIFY_ACL, null);
  }

  /**
   * Helper function to get all application acls
   * @return a map of {@link ApplicationAccessType} to {@link String} for all the acls defined
   */
  public Map<ApplicationAccessType, String> getYarnApplicationAcls() {
    Map<ApplicationAccessType, String> acls = new HashMap<>();
    String viewAcl = getYarnApplicationViewAcl();
    String modifyAcl = getYarnApplicationModifyAcl();
    if (viewAcl != null) {
      acls.put(ApplicationAccessType.VIEW_APP, viewAcl);
    }
    if (modifyAcl != null) {
      acls.put(ApplicationAccessType.MODIFY_APP, modifyAcl);
    }
    return acls;
  }

}
