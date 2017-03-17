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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.JobGraph;
import org.apache.samza.execution.JobNode;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RemoteApplicationRunner.class);

  private final StreamManager streamManager;

  public RemoteApplicationRunner(Config config) {
    super(config);
    this.streamManager = new StreamManager(new JavaSystemConfig(config).getSystemAdmins());
  }

  /**
   * Run the {@link StreamApplication} on the remote cluster
   * @param app a StreamApplication
   */
  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      JobGraph jobGraph = getExecutionPlan(app);

      // 2. create the necessary streams
      List<StreamSpec> streams = jobGraph.getIntermediateStreams().stream()
          .map(streamEdge -> streamEdge.getStreamSpec())
          .collect(Collectors.toList());
      streamManager.createStreams(streams);

      // 3. submit jobs for remote execution
      jobGraph.getJobNodes().forEach(job -> {
          Config jobConfig = job.generateConfig();
          log.info("Starting job {} with config {}", job.getId(), jobConfig);
          JobRunner runner = new JobRunner(jobConfig);
          runner.run(true);
        });
    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    }
  }

  @Override
  public void kill(StreamApplication app) {
    try {
      JobGraph jobGraph = getExecutionPlan(app);

      jobGraph.getJobNodes().forEach(job -> {
          Config jobConfig = job.generateConfig();
          log.info("Killing job {}", job.getId());
          JobRunner runner = new JobRunner(jobConfig);
          runner.kill();
        });
    } catch (Throwable t) {
      throw new SamzaException("Failed to kill application", t);
    }
  }

  @Override
  public ApplicationStatus status(StreamApplication app) {
    try {
      boolean finished = false;
      boolean unsuccessfulFinish = false;

      JobGraph jobGraph = getExecutionPlan(app);
      for (JobNode job : jobGraph.getJobNodes()) {
        Config jobConfig = job.generateConfig();
        JobRunner runner = new JobRunner(jobConfig);
        ApplicationStatus status = runner.status();
        log.debug("Status is {} for jopb {}", new Object[]{status, job.getId()});

        switch (status) {
          case Running:
            return ApplicationStatus.Running;
          case UnsuccessfulFinish:
            unsuccessfulFinish = true;
          case SuccessfulFinish:
            finished = true;
            break;
          default:
            // Do nothing
        }
      }

      if (unsuccessfulFinish) {
        return ApplicationStatus.UnsuccessfulFinish;
      } else if (finished) {
        return ApplicationStatus.SuccessfulFinish;
      }
      return ApplicationStatus.New;
    } catch (Throwable t) {
      throw new SamzaException("Failed to get status for application", t);
    }
  }

  private JobGraph getExecutionPlan(StreamApplication app) throws Exception {
    // build stream graph
    StreamGraphImpl streamGraph = new StreamGraphImpl(this, config);
    app.init(streamGraph, config);

    // create the physical execution plan
    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    return planner.plan(streamGraph);
  }
}
