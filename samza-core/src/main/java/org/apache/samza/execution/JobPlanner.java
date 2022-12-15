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
package org.apache.samza.execution;

import com.google.common.base.Strings;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationApiType;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a temporary helper class to include all common logic to generate {@link JobConfig}s for high- and low-level
 * applications in {@link org.apache.samza.runtime.LocalApplicationRunner} and {@link org.apache.samza.runtime.RemoteApplicationRunner}.
 *
 * TODO: Fix SAMZA-1811 to consolidate this class with {@link ExecutionPlanner}
 */
public abstract class JobPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(JobPlanner.class);

  protected final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;
  protected final Config userConfig;

  JobPlanner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> descriptor) {
    this.appDesc = descriptor;
    this.userConfig = descriptor.getConfig();
  }

  public abstract List<JobConfig> prepareJobs();

  StreamManager buildAndStartStreamManager(Config config) {
    StreamManager streamManager = new StreamManager(config);
    streamManager.start();
    return streamManager;
  }

  ExecutionPlan getExecutionPlan() {
    return getExecutionPlan(null);
  }

  /* package private */
  ExecutionPlan getExecutionPlan(String runId) {
    Map<String, String> allowedUserConfig = new HashMap<>(userConfig);
    Map<String, String> generatedConfig = new HashMap<>();

    // TODO: This should all be consolidated with ExecutionPlanner after fixing SAMZA-1811
    // Don't generate any configurations for LegacyTaskApplications
    if (!LegacyTaskApplication.class.isAssignableFrom(appDesc.getAppClass())) {
      // Don't allow overriding task.inputs to a blank string
      if (StringUtils.isBlank(userConfig.get(TaskConfig.INPUT_STREAMS))) {
        allowedUserConfig.remove(TaskConfig.INPUT_STREAMS);
      }
      generatedConfig.putAll(getGeneratedConfig());
    }

    if (ApplicationConfig.ApplicationMode.BATCH.name().equals(generatedConfig.get(ApplicationConfig.APP_MODE))) {
      allowedUserConfig.remove(ClusterManagerConfig.JOB_HOST_AFFINITY_ENABLED);
    }

    // APP_RUN_ID should be generated for both LegacyTaskApplications & descriptor based applications
    // This config is used in BATCH mode to create new intermediate streams on runs and in stream mode use by
    // Container Placements to identify a deployment of Samza
    if (StringUtils.isNoneEmpty(runId)) {
      generatedConfig.put(ApplicationConfig.APP_RUN_ID, runId);
    }

    // Assign app.api.type config
    if (StreamApplication.class.isAssignableFrom(appDesc.getAppClass())) {
      generatedConfig.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.HIGH_LEVEL.name());
    } else if (TaskApplication.class.isAssignableFrom(appDesc.getAppClass())) {
      generatedConfig.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LOW_LEVEL.name());
    } else if (LegacyTaskApplication.class.isAssignableFrom(appDesc.getAppClass())) {
      generatedConfig.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LEGACY.name());
    } else {
      final TaskConfig taskConfig = new TaskConfig(userConfig);
      final Optional<String> taskClassOption = taskConfig.getTaskClass();
      if (!taskClassOption.isPresent() || Strings.isNullOrEmpty(taskClassOption.get())) {
        // no task.class defined either. This is wrong.
        throw new ConfigException("Legacy task applications must set a non-empty task.class in configuration.");
      }
      generatedConfig.put(ApplicationConfig.APP_API_TYPE, ApplicationApiType.LEGACY.name());
    }

    // merge user-provided configuration with generated configuration. generated configuration has lower priority.
    Config mergedConfig = JobNodeConfigurationGenerator.mergeConfig(allowedUserConfig, generatedConfig);

    // creating the StreamManager to get all input/output streams' metadata for planning
    StreamManager streamManager = buildAndStartStreamManager(mergedConfig);

    try {
      ExecutionPlanner planner = new ExecutionPlanner(mergedConfig, streamManager);
      return planner.plan(appDesc);
    } finally {
      streamManager.stop();
    }
  }

  /**
   * Write the execution plan JSON to a file
   * @param planJson JSON representation of the plan
   */
  final void writePlanJsonFile(String planJson) {
    try {
      String content = "plan='" + planJson + "'";
      String planPath = System.getenv(ShellCommandConfig.EXECUTION_PLAN_DIR);
      if (planPath != null && !planPath.isEmpty()) {
        // Write the plan json to plan path
        File file = new File(planPath + "/plan.json");
        file.setReadable(true, false);
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(content);
        writer.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to write execution plan json to file", e);
    }
  }

  private Map<String, String> getGeneratedConfig() {
    Map<String, String> generatedConfig = new HashMap<>();

    Map<String, String> systemStreamConfigs = generateSystemStreamConfigs(appDesc);
    generatedConfig.putAll(systemStreamConfigs);

    StreamConfig streamConfig = new StreamConfig(new MapConfig(generatedConfig));
    Set<String> inputStreamIds = new HashSet<>(appDesc.getInputStreamIds());
    inputStreamIds.removeAll(appDesc.getOutputStreamIds()); // exclude intermediate streams

    final ApplicationConfig.ApplicationMode mode;
    if (inputStreamIds.isEmpty()) {
      mode = ApplicationConfig.ApplicationMode.STREAM; // use stream by default
    } else {
      mode = inputStreamIds.stream().allMatch(streamConfig::getIsBounded)
          ? ApplicationConfig.ApplicationMode.BATCH
          : ApplicationConfig.ApplicationMode.STREAM;
    }

    generatedConfig.put(ApplicationConfig.APP_MODE, mode.name());

    // adding app.class in the configuration, unless it is LegacyTaskApplication
    if (!LegacyTaskApplication.class.getName().equals(appDesc.getAppClass().getName())) {
      generatedConfig.put(ApplicationConfig.APP_CLASS, appDesc.getAppClass().getName());
    }
    return generatedConfig;
  }

  private Map<String, String> generateSystemStreamConfigs(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc) {
    Map<String, String> systemStreamConfigs = new HashMap<>();
    appDesc.getInputDescriptors().forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    appDesc.getOutputDescriptors().forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    appDesc.getSystemDescriptors().forEach(sd -> systemStreamConfigs.putAll(sd.toConfig()));
    appDesc.getDefaultSystemDescriptor().ifPresent(dsd ->
        systemStreamConfigs.put(JobConfig.JOB_DEFAULT_SYSTEM, dsd.getSystemName()));
    return systemStreamConfigs;
  }

  /**
   * Generates configs for a single job in app, job.id from app.id and job.name from app.name config
   * If both job.id and app.id is defined, app.id takes precedence and job.id is set to value of app.id
   * If both job.name and app.name is defined, app.name takes precedence and job.name is set to value of app.name
   *
   * @param userConfigs configs passed from user
   *
   */
  public static MapConfig generateSingleJobConfig(Map<String, String> userConfigs) {
    Map<String, String> generatedConfig = new HashMap<>(userConfigs);

    if (!userConfigs.containsKey(JobConfig.JOB_NAME) && !userConfigs.containsKey(ApplicationConfig.APP_NAME)) {
      throw new SamzaException("Samza app name should not be null, Please set either app.name (preferred) or job.name (deprecated) in configs");
    }

    if (userConfigs.containsKey(JobConfig.JOB_ID)) {
      LOG.warn("{} is a deprecated configuration, use app.id instead.", JobConfig.JOB_ID);
    }

    if (userConfigs.containsKey(JobConfig.JOB_NAME)) {
      LOG.warn("{} is a deprecated configuration, use use app.name instead.", JobConfig.JOB_NAME);
    }

    if (userConfigs.containsKey(ApplicationConfig.APP_NAME)) {
      String appName =  userConfigs.get(ApplicationConfig.APP_NAME);
      LOG.info("app.name is defined, generating job.name equal to app.name value: {}", appName);
      generatedConfig.put(JobConfig.JOB_NAME, appName);
    }

    if (userConfigs.containsKey(ApplicationConfig.APP_ID)) {
      String appId =  userConfigs.get(ApplicationConfig.APP_ID);
      LOG.info("app.id is defined, generating job.id equal to app.name value: {}", appId);
      generatedConfig.put(JobConfig.JOB_ID, appId);
    }

    return new MapConfig(generatedConfig);
  }

}
