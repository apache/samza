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

import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API
 */
public abstract class AbstractApplicationRunner extends ApplicationRunner {

  private final StreamManager streamManager;
  private final ExecutionPlanner planner;

  public AbstractApplicationRunner(Config config) {
    super(config);
    this.streamManager = new StreamManager(new JavaSystemConfig(config).getSystemAdmins());
    this.planner = new ExecutionPlanner(config, streamManager);
  }

  @Override
  public StreamSpec getStreamSpec(String streamId) {
    StreamConfig streamConfig = new StreamConfig(config);
    String physicalName = streamConfig.getPhysicalName(streamId);
    return getStreamSpec(streamId, physicalName);
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

    return new StreamSpec(streamId, physicalName, system, properties);
  }

  final ExecutionPlan getExecutionPlan(StreamApplication app) throws Exception {
    // build stream graph
    StreamGraphImpl streamGraph = new StreamGraphImpl(this, config);
    app.init(streamGraph, config);

    // create the physical execution plan
    return planner.plan(streamGraph);
  }

  final StreamManager getStreamManager() {
    return streamManager;
  }
}
