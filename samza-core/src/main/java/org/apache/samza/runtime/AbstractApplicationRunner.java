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
import java.util.Map;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.AsyncStreamTaskApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInternal;
import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.App;


/**
 * Defines common, core behavior for implementations of the {@link ApplicationRunner} API
 */
public abstract class AbstractApplicationRunner extends ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(AbstractApplicationRunner.class);

  private final StreamManager streamManager;
  private final ExecutionPlanner planner;

  public AbstractApplicationRunner(Config config) {
    super(config);
    this.streamManager = new StreamManager(new JavaSystemConfig(config).getSystemAdmins());
    this.planner = new ExecutionPlanner(config, streamManager);
  }

//  /**
//   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
//   *
//   * The stream configurations are read from the following properties in the config:
//   * {@code streams.{$streamId}.*}
//   * <br>
//   * All properties matching this pattern are assumed to be system-specific with one exception. The following
//   * property is a Samza property which is used to bind the stream to a system.
//   *
//   * <ul>
//   *   <li>samza.system - The name of the System on which this stream will be used. If this property isn't defined
//   *                      the stream will be associated with the System defined in {@code job.default.system}</li>
//   * </ul>
//   *
//   * @param streamId      The logical identifier for the stream in Samza.
//   * @param physicalName  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
//   * @return              The {@link StreamSpec} instance.
//   */
//  /*package private*/ StreamSpec getStreamSpec(String streamId, String physicalName) {
//    StreamConfig streamConfig = new StreamConfig(config);
//    String system = streamConfig.getSystem(streamId);
//
//    return getStreamSpec(streamId, physicalName, system);
//  }
//
//  /**
//   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
//   *
//   * The stream configurations are read from the following properties in the config:
//   * {@code streams.{$streamId}.*}
//   *
//   * @param streamId      The logical identifier for the stream in Samza.
//   * @param physicalName  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
//   * @param system        The name of the System on which this stream will be used.
//   * @return              The {@link StreamSpec} instance.
//   */
//  /*package private*/ StreamSpec getStreamSpec(String streamId, String physicalName, String system) {
//    StreamConfig streamConfig = new StreamConfig(config);
//    Map<String, String> properties = streamConfig.getStreamProperties(streamId);
//
//    return new StreamSpec(streamId, physicalName, system, properties);
//  }

  final ExecutionPlan getExecutionPlan(StreamApplication app) throws Exception {
    // create the physical execution plan
    return planner.plan(new StreamApplicationInternal(app).getStreamGraphImpl());
  }

  final StreamManager getStreamManager() {
    return streamManager;
  }

  @Override
  public final StreamGraph createGraph() {
    return new StreamGraphImpl(config);
  }

  abstract void runStreamApp(StreamApplication app);

  abstract void runStreamTask(StreamTaskApplication app);

  abstract void runAsyncStreamTask(AsyncStreamTaskApplication app);

  abstract void killStreamApp(StreamApplication app);

  abstract void killStreamTask(StreamTaskApplication app);

  abstract void killAsyncStreamTask(AsyncStreamTaskApplication app);

  abstract ApplicationStatus statusStreamApp(StreamApplication app);

  abstract ApplicationStatus statusStreamTask(StreamTaskApplication app);

  abstract ApplicationStatus statusAsyncStreamTask(AsyncStreamTaskApplication app);

  @Override
  public void run(ApplicationBase streamApp) {
    if (streamApp instanceof StreamApplication) {
      this.runStreamApp((StreamApplication) streamApp);
    }
    if (streamApp instanceof StreamTaskApplication) {
      this.runStreamTask((StreamTaskApplication) streamApp);
    }
    if (streamApp instanceof AsyncStreamTaskApplication) {
      this.runAsyncStreamTask((AsyncStreamTaskApplication) streamApp);
    }
    throw new IllegalArgumentException("Unsupported application class: " + streamApp.getClass().getCanonicalName());
  }

  @Override
  public void kill(ApplicationBase streamApp) {
    if (streamApp instanceof StreamApplication) {
      this.killStreamApp((StreamApplication) streamApp);
    }
    if (streamApp instanceof StreamTaskApplication) {
      this.killStreamTask((StreamTaskApplication) streamApp);
    }
    if (streamApp instanceof AsyncStreamTaskApplication) {
      this.killAsyncStreamTask((AsyncStreamTaskApplication) streamApp);
    }
    throw new IllegalArgumentException("Unsupported application class: " + streamApp.getClass().getCanonicalName());
  }

  @Override
  public ApplicationStatus status(ApplicationBase streamApp) {
    if (streamApp instanceof StreamApplication) {
      return this.statusStreamApp((StreamApplication) streamApp);
    }
    if (streamApp instanceof StreamTaskApplication) {
      return this.statusStreamTask((StreamTaskApplication) streamApp);
    }
    if (streamApp instanceof AsyncStreamTaskApplication) {
      return this.statusAsyncStreamTask((AsyncStreamTaskApplication) streamApp);
    }
    throw new IllegalArgumentException("Unsupported application class: " + streamApp.getClass().getCanonicalName());
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
