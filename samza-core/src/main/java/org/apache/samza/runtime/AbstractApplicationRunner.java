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

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API
 */
public abstract class AbstractApplicationRunner extends ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(AbstractApplicationRunner.class);

  private final StreamManager streamManager;
  private final SystemAdmins systemAdmins;

  public AbstractApplicationRunner(Config config) {
    super(config);
    systemAdmins = new SystemAdmins(config);
    this.streamManager = new StreamManager(systemAdmins);
  }

  @Override
  public StreamSpec getStreamSpec(String streamId) {
    StreamConfig streamConfig = new StreamConfig(config);
    String physicalName = streamConfig.getPhysicalName(streamId);
    return getStreamSpec(streamId, physicalName);
  }

  @Override
  public void run(StreamApplication streamApp) {
    systemAdmins.start();
  }

  @Override
  public void kill(StreamApplication streamApp) {
    systemAdmins.start();
  }

  /**
   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
   *
   * The stream configurations are read from the following properties in the config:
   * {@code streams.{$streamId}.*}
   * <br>
   * All properties matching this pattern are assumed to be system-specific with one exception. The following
   * property is a Samza property which is used to bind the stream to a system.
   *
   * <ul>
   *   <li>samza.system - The name of the System on which this stream will be used. If this property isn't defined
   *                      the stream will be associated with the System defined in {@code job.default.system}</li>
   * </ul>
   *
   * @param streamId      The logical identifier for the stream in Samza.
   * @param physicalName  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
   * @return              The {@link StreamSpec} instance.
   */
  /*package private*/ StreamSpec getStreamSpec(String streamId, String physicalName) {
    StreamConfig streamConfig = new StreamConfig(config);
    String system = streamConfig.getSystem(streamId);

    return getStreamSpec(streamId, physicalName, system);
  }

  /**
   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
   *
   * The stream configurations are read from the following properties in the config:
   * {@code streams.{$streamId}.*}
   *
   * @param streamId      The logical identifier for the stream in Samza.
   * @param physicalName  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
   * @param system        The name of the System on which this stream will be used.
   * @return              The {@link StreamSpec} instance.
   */
  /*package private*/ StreamSpec getStreamSpec(String streamId, String physicalName, String system) {
    StreamConfig streamConfig = new StreamConfig(config);
    Map<String, String> properties = streamConfig.getStreamProperties(streamId);
    boolean isBounded = streamConfig.getIsBounded(streamId);

    return new StreamSpec(streamId, physicalName, system, isBounded, properties);
  }

  /* package private */
  ExecutionPlan getExecutionPlan(StreamApplication app) throws Exception {
    return getExecutionPlan(app, null);
  }

  /* package private */
  ExecutionPlan getExecutionPlan(StreamApplication app, String runId) throws Exception {
    // build stream graph
    StreamGraphImpl streamGraph = new StreamGraphImpl(this, config);
    app.init(streamGraph, config);

    // create the physical execution plan
    Map<String, String> cfg = new HashMap<>(config);
    if (StringUtils.isNoneEmpty(runId)) {
      cfg.put(ApplicationConfig.APP_RUN_ID, runId);
    }

    Set<StreamSpec> inputStreams = new HashSet<>(streamGraph.getInputOperators().keySet());
    inputStreams.removeAll(streamGraph.getOutputStreams().keySet());
    ApplicationMode mode = inputStreams.stream().allMatch(StreamSpec::isBounded)
        ? ApplicationMode.BATCH : ApplicationMode.STREAM;
    cfg.put(ApplicationConfig.APP_MODE, mode.name());

    ExecutionPlanner planner = new ExecutionPlanner(new MapConfig(cfg), streamManager);
    return planner.plan(streamGraph);
  }

  /* package private for testing */
  StreamManager getStreamManager() {
    return streamManager;
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
}
