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

import java.io.File;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API.
 */
public abstract class AbstractApplicationRunner implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(AbstractApplicationRunner.class);

  protected final Config config;
  protected final Map<String, MetricsReporter> metricsReporters = new HashMap<>();

  AbstractApplicationRunner(Config config) {
    this.config = config;
  }

  @Override
  public final void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
    this.metricsReporters.putAll(metricsReporters);
  }

  @Override
  public final void run(ApplicationSpec appSpec) {
    getAppRuntimeExecutable(appSpec).run();
  }

  @Override
  public final ApplicationStatus status(ApplicationSpec appSpec) {
    return getAppRuntimeExecutable(appSpec).status();
  }

  @Override
  public final void kill(ApplicationSpec appSpec) {
    getAppRuntimeExecutable(appSpec).kill();
  }

  @Deprecated
  @Override
  public final void waitForFinish(ApplicationSpec appSpec) {
    getAppRuntimeExecutable(appSpec).waitForFinish(Duration.ofSeconds(0));
  }

  @Override
  public final boolean waitForFinish(ApplicationSpec appSpec, Duration timeout) {
    return getAppRuntimeExecutable(appSpec).waitForFinish(timeout);
  }

  interface AppRuntimeExecutable {

    void run();

    void kill();

    ApplicationStatus status();

    /**
     * Waits for {@code timeout} duration for the application to finish.
     *
     * @param timeout time to wait for the application to finish
     * @return true - application finished before timeout
     *         false - otherwise
     */
    boolean waitForFinish(Duration timeout);

  }

  abstract AppRuntimeExecutable getTaskAppRuntimeExecutable(TaskAppSpecImpl appSpec);

  abstract AppRuntimeExecutable getStreamAppRuntimeExecutable(StreamAppSpecImpl appSpec);

  final StreamManager buildAndStartStreamManager() {
    StreamManager streamManager = new StreamManager(this.config);
    streamManager.start();
    return streamManager;
  }

  final ExecutionPlan getExecutionPlan(OperatorSpecGraph graphSpec, StreamManager streamManager) throws Exception {
    return getExecutionPlan(graphSpec, null, streamManager);
  }

  /* package private */
  final ExecutionPlan getExecutionPlan(OperatorSpecGraph specGraph, String runId, StreamManager streamManager) throws Exception {

    // update application configs
    Map<String, String> cfg = new HashMap<>(config);
    if (StringUtils.isNoneEmpty(runId)) {
      cfg.put(ApplicationConfig.APP_RUN_ID, runId);
    }

    StreamConfig streamConfig = new StreamConfig(config);
    Set<String> inputStreams = new HashSet<>(specGraph.getInputOperators().keySet());
    inputStreams.removeAll(specGraph.getOutputStreams().keySet());
    ApplicationMode mode = inputStreams.stream().allMatch(streamConfig::getIsBounded)
        ? ApplicationMode.BATCH : ApplicationMode.STREAM;
    cfg.put(ApplicationConfig.APP_MODE, mode.name());

    // create the physical execution plan
    ExecutionPlanner planner = new ExecutionPlanner(new MapConfig(cfg), streamManager);
    return planner.plan(specGraph);
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

  private AppRuntimeExecutable getAppRuntimeExecutable(ApplicationSpec appSpec) {
    if (appSpec instanceof StreamAppSpecImpl) {
      return getStreamAppRuntimeExecutable((StreamAppSpecImpl) appSpec);
    }
    if (appSpec instanceof TaskAppSpecImpl) {
      return getTaskAppRuntimeExecutable((TaskAppSpecImpl) appSpec);
    }
    throw new IllegalArgumentException(String.format("The specified application %s is not valid. "
        + "Only StreamApplicationSpec and TaskApplicationSpec are supported.", appSpec.getClass().getName()));
  }

}
