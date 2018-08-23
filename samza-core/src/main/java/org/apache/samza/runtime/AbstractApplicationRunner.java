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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.AppDescriptorImpl;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ApplicationDescriptors;
import org.apache.samza.application.StreamAppDescriptorImpl;
import org.apache.samza.application.TaskAppDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API.
 */
public abstract class AbstractApplicationRunner implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(AbstractApplicationRunner.class);

  protected final AppDescriptorImpl appDesc;
  protected final Config config;
  protected final Map<String, MetricsReporter> metricsReporters = new HashMap<>();

  /**
   * This is a temporary helper class to include all common logic to generate {@link JobConfig}s for high- and low-level
   * applications in {@link LocalApplicationRunner} and {@link RemoteApplicationRunner}.
   *
   * TODO: Fix SAMZA-1811 to consolidate the planning into {@link ExecutionPlanner}
   */
  static abstract class JobPlanner {

    protected final AppDescriptorImpl appDesc;
    protected final Config config;

    JobPlanner(AppDescriptorImpl descriptor) {
      this.appDesc = descriptor;
      this.config = descriptor.getConfig();
    }

    abstract List<JobConfig> prepareStreamJobs(StreamAppDescriptorImpl streamAppDesc) throws Exception;

    List<JobConfig> prepareJobs() throws Exception {
      String appId = new ApplicationConfig(appDesc.getConfig()).getGlobalAppId();
      return ApplicationDescriptors.forType(
          taskAppDesc -> {
          try {
            return Collections.singletonList(JobPlanner.this.prepareTaskJob(taskAppDesc));
          } catch (Exception e) {
            throw new SamzaException("Failed to generate JobConfig for TaskApplication " + appId, e);
          }
        },
          streamAppDesc -> {
          try {
            return JobPlanner.this.prepareStreamJobs(streamAppDesc);
          } catch (Exception e) {
            throw new SamzaException("Failed to generate JobConfig for StreamApplication " + appId, e);
          }
        },
        appDesc);
    }

    StreamManager buildAndStartStreamManager() {
      StreamManager streamManager = new StreamManager(config);
      streamManager.start();
      return streamManager;
    }

    ExecutionPlan getExecutionPlan(OperatorSpecGraph specGraph, StreamManager streamManager) throws Exception {
      return getExecutionPlan(specGraph, null, streamManager);
    }

    /* package private */
    ExecutionPlan getExecutionPlan(OperatorSpecGraph specGraph, String runId, StreamManager streamManager) throws Exception {

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
      validateAppClassCfg(cfg, appDesc.getAppClass());

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

    // helper method to generate a single node job configuration for low level task applications
    private JobConfig prepareTaskJob(TaskAppDescriptorImpl taskAppDesc) {
      Map<String, String> cfg = new HashMap<>(taskAppDesc.getConfig());
      //TODO: add stream and system descriptor to configuration conversion here when SAMZA-1804 is fixed.
      // adding table configuration
      List<TableSpec> tableSpecs = taskAppDesc.getTables().stream()
          .map(td -> ((BaseTableDescriptor) td).getTableSpec())
          .collect(Collectors.toList());
      cfg.putAll(TableConfigGenerator.generateConfigsForTableSpecs(tableSpecs));
      validateAppClassCfg(cfg, taskAppDesc.getAppClass());
      return new JobConfig(new MapConfig(cfg));
    }

    private void validateAppClassCfg(Map<String, String> cfg, Class<? extends ApplicationBase> appClass) {
      if (StringUtils.isNotBlank(cfg.get(ApplicationConfig.APP_CLASS))) {
        // app.class is already set
        return;
      }
      // adding app.class in the configuration
      cfg.put(ApplicationConfig.APP_CLASS, appClass.getCanonicalName());
    }
  }

  AbstractApplicationRunner(AppDescriptorImpl appDesc) {
    this.appDesc = appDesc;
    this.config = appDesc.getConfig();
  }

  @Override
  public final void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
    this.metricsReporters.putAll(metricsReporters);
  }

}
