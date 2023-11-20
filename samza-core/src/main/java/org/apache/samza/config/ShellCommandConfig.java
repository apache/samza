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

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;


public class ShellCommandConfig extends MapConfig {
  /**
   * This environment variable is used to store a JSON serialized map of all coordinator system configs.
   */
  public static final String ENV_COORDINATOR_SYSTEM_CONFIG = "SAMZA_COORDINATOR_SYSTEM_CONFIG";

  /**
   * This environment variable is used to pass a JSON serialized map of configs provided during job submission.
   */
  public static final String ENV_SUBMISSION_CONFIG = "SAMZA_SUBMISSION_CONFIG";

  /**
   * The ID for a container. This is a string representation that is unique to the runtime environment.
   */
  public static final String ENV_CONTAINER_ID = "SAMZA_CONTAINER_ID";

  /**
   * The URL location of the job coordinator's HTTP server.
   */
  public static final String ENV_COORDINATOR_URL = "SAMZA_COORDINATOR_URL";

  /**
   * Arguments to be passed to the processing running the TaskRunner (or equivalent, for non JVM languages).
   */
  public static final String ENV_JAVA_OPTS = "JAVA_OPTS";

  /**
   * The JAVA_HOME path for running the task
   */
  public static final String ENV_JAVA_HOME = "JAVA_HOME";

  /**
   * The ID assigned to the container by the execution environment (eg: YARN Container Id)
   */
  public static final String ENV_EXECUTION_ENV_CONTAINER_ID = "EXECUTION_ENV_CONTAINER_ID";

  /*
   * The base directory for storing logged data stores used in Samza. This has to be set on all machine running Samza
   * containers. For example, when using YARN, it has to be set in all NMs and passed to the containers.
   * If this environment variable is not set, the path defaults to current working directory (which is the same as the
   * path for persisting non-logged data stores)
   */
  public static final String ENV_LOGGED_STORE_BASE_DIR = "LOGGED_STORE_BASE_DIR";

  /**
   * The directory path that contains the execution plan
   */
  public static final String EXECUTION_PLAN_DIR = "EXECUTION_PLAN_DIR";

  /**
   * Points to the lib directory of the localized resources(other than the framework dependencies).
   */
  public static final String ENV_ADDITIONAL_CLASSPATH_DIR = "ADDITIONAL_CLASSPATH_DIR";

  public static final String COMMAND_SHELL_EXECUTE = "task.execute";
  public static final String TASK_JVM_OPTS = "task.opts";
  public static final String WORKER_JVM_OPTS = "worker.opts";
  public static final String TASK_JAVA_HOME = "task.java.home";

  /**
   * SamzaContainer uses JARs from the lib directory of the framework in it classpath. In some cases, it is necessary to include
   * the jars from lib directories of the resources that are localized along with the framework dependencies. These resources are logically
   * independent of the framework and cannot be bundled with the framework dependencies. The URI of these resources are set dynamically at
   * run-time before launching the SamzaContainer. This environment variable can be set to a lib directory of the localized resource and
   * it will be included in the java classpath of the SamzaContainer.
   */
  public static final String ADDITIONAL_CLASSPATH_DIR = "additional.classpath.dir";

  public ShellCommandConfig(Config config) {
    super(config);
  }

  public String getCommand() {
    return Optional.ofNullable(get(ShellCommandConfig.COMMAND_SHELL_EXECUTE)).orElse("bin/run-container.sh");
  }

  public Optional<String> getTaskOpts() {
    String taskOpts = get(ShellCommandConfig.TASK_JVM_OPTS);
    String autosizingContainerMaxHeap = get(JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_HEAP_MB);

    return Optional.ofNullable(getFinalJvmOptions(taskOpts, autosizingContainerMaxHeap));
  }

  /**
   * Returns the worker opts for the application if available.
   */
  public Optional<String> getWorkerOpts() {
    String autosizingWorkerHeapMb = get(JobConfig.JOB_AUTOSIZING_WORKER_MAX_HEAP_MB);
    String workerOpts = get(ShellCommandConfig.WORKER_JVM_OPTS);
    return Optional.ofNullable(getFinalJvmOptions(workerOpts, autosizingWorkerHeapMb));
  }

  public Optional<String> getJavaHome() {
    return Optional.ofNullable(get(ShellCommandConfig.TASK_JAVA_HOME));
  }

  public Optional<String> getAdditionalClasspathDir() {
    return Optional.ofNullable(get(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR));
  }

  /**
   * Returns the final JVM options by applying the heap override if available to the jvm opts
   */
  @VisibleForTesting
  String getFinalJvmOptions(String jvmOpts, String maxHeapOverride) {
    String finalJvmOpts = jvmOpts;
    if (new JobConfig(this).getAutosizingEnabled() && StringUtils.isNotEmpty(maxHeapOverride)) {
      String xmxSetting = "-Xmx" + maxHeapOverride + "m";
      if (StringUtils.isNotBlank(jvmOpts)) {
        if (jvmOpts.contains("-Xmx")) {
          finalJvmOpts = jvmOpts.replaceAll("-Xmx\\S+", xmxSetting);
        } else {
          finalJvmOpts = jvmOpts.concat(" " + xmxSetting);
        }
      } else {
        finalJvmOpts = xmxSetting;
      }
    }

    return finalJvmOpts;
  }
}
