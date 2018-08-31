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
package org.apache.samza.runtime;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.JobNode;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.StreamGraphSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API.
 */
public abstract class AbstractApplicationRunner extends ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(AbstractApplicationRunner.class);

  /**
   * The {@link ApplicationRunner} is supposed to run a single {@link StreamApplication} instance in the full life-cycle
   */
  protected final StreamGraphSpec graphSpec;

  public AbstractApplicationRunner(Config config) {
    super(config);
    this.graphSpec = new StreamGraphSpec(config);
  }

  public ExecutionPlan getExecutionPlan(StreamApplication app) throws Exception {
    return getExecutionPlan(app, null);
  }

  /* package private */
  ExecutionPlan getExecutionPlan(StreamApplication app, String runId) throws Exception {
    // build stream graph
    app.init(graphSpec, config);
    OperatorSpecGraph specGraph = graphSpec.getOperatorSpecGraph();

    // generated application configs are stored in cfg
    Map<String, String> cfg = new HashMap<>();
    if (StringUtils.isNoneEmpty(runId)) {
      cfg.put(ApplicationConfig.APP_RUN_ID, runId);
    }

    StreamConfig streamConfig = new StreamConfig(config);
    Set<String> inputStreams = new HashSet<>(specGraph.getInputOperators().keySet());
    inputStreams.removeAll(specGraph.getOutputStreams().keySet());
    ApplicationMode mode = inputStreams.stream().allMatch(streamConfig::getIsBounded)
        ? ApplicationMode.BATCH : ApplicationMode.STREAM;
    cfg.put(ApplicationConfig.APP_MODE, mode.name());

    // merge user-provided configuration with input/output descriptor generated configuration
    // descriptor generated configuration has higher priority
    Map<String, String> systemStreamConfigs = new HashMap<>();
    graphSpec.getInputDescriptors().forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    graphSpec.getOutputDescriptors().forEach((key, value) -> systemStreamConfigs.putAll(value.toConfig()));
    graphSpec.getSystemDescriptors().forEach(sd -> systemStreamConfigs.putAll(sd.toConfig()));
    graphSpec.getDefaultSystemDescriptor().ifPresent(dsd ->
        systemStreamConfigs.put(JobConfig.JOB_DEFAULT_SYSTEM(), dsd.getSystemName()));
    cfg.putAll(systemStreamConfigs);

    // create the physical execution plan and merge with overrides. This works for a single-stage job now
    // TODO: This should all be consolidated with ExecutionPlanner after fixing SAMZA-1811
    Config mergedConfig = JobNode.mergeJobConfig(config, new MapConfig(cfg));
    StreamManager streamManager = buildAndStartStreamManager(mergedConfig);
    try {
      ExecutionPlanner planner = new ExecutionPlanner(mergedConfig, streamManager);
      return planner.plan(specGraph);
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
      String planPath = System.getenv(ShellCommandConfig.EXECUTION_PLAN_DIR());
      if (planPath != null && !planPath.isEmpty()) {
        // Write the plan json to plan path
        File file = new File(planPath + "/plan.json");
        file.setReadable(true, false);
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(content);
        writer.close();
      }
    } catch (Exception e) {
      log.warn("Failed to write execution plan json to file", e);
    }
  }

  @VisibleForTesting
  StreamManager buildAndStartStreamManager(Config config) {
    StreamManager streamManager = new StreamManager(config);
    streamManager.start();
    return streamManager;
  }
}
