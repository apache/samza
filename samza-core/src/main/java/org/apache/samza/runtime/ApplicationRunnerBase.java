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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInternal;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API
 */
public abstract class ApplicationRunnerBase implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(ApplicationRunnerBase.class);

  protected final Config config;
  protected final Map<String, MetricsReporter> metricsReporters = new HashMap<>();

  public ApplicationRunnerBase(Config config) {
    this.config = config;
  }

  @Override
  public final StreamGraph createGraph() {
    return new StreamGraphImpl(config);
  }

  interface ApplicationRuntimeInstance {
    void run();
    void kill();
    ApplicationStatus status();
    void waitForFinish();
  }

  abstract ApplicationRuntimeInstance getRuntimeInstance(ApplicationBase app);

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

  @Override
  public final void run(ApplicationBase app) {
    this.getRuntimeInstance(app).run();
  }

  @Override
  public final void kill(ApplicationBase app) {
    this.getRuntimeInstance(app).kill();
  }

  @Override
  public final ApplicationStatus status(ApplicationBase app) {
    return this.getRuntimeInstance(app).status();
  }

  @Override
  public final void waitForFinish(ApplicationBase app) {
    this.getRuntimeInstance(app).waitForFinish();
  }

  @Override
  public final void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
    this.metricsReporters.putAll(metricsReporters);
  }

}
