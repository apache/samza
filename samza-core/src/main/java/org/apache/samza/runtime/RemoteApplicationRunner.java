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
import org.apache.samza.execution.ProcessorGraph;
import org.apache.samza.execution.ProcessorNode;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RemoteApplicationRunner.class);

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  /**
   * Run the {@link StreamApplication} on the remote cluster
   * @param app a StreamApplication
   */
  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      ProcessorGraph processorGraph = getExecutionPlan(app);

      // 2. create the necessary streams
      List<StreamSpec> streams = processorGraph.getIntermediateStreams().stream()
          .map(streamEdge -> streamEdge.getStreamSpec())
          .collect(Collectors.toList());
      StreamManager streamManager = new StreamManager(new JavaSystemConfig(config).getSystemAdmins());
      streamManager.createStreams(streams);

      // 3. submit jobs for remote execution
      processorGraph.getProcessorNodes().forEach(processor -> {
          Config processorConfig = processor.generateConfig();
          log.info("Starting processor {} with config {}", processor.getId(), processorConfig);
          JobRunner runner = new JobRunner(processorConfig);
          runner.run(true);
        });
    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    }
  }

  @Override
  public void kill(StreamApplication app) {
    try {
      ProcessorGraph processorGraph = getExecutionPlan(app);

      processorGraph.getProcessorNodes().forEach(processor -> {
          Config processorConfig = processor.generateConfig();
          log.info("Killing processor {}", processor.getId());
          JobRunner runner = new JobRunner(processorConfig);
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

      ProcessorGraph processorGraph = getExecutionPlan(app);
      for (ProcessorNode processor : processorGraph.getProcessorNodes()) {
        Config processorConfig = processor.generateConfig();
        JobRunner runner = new JobRunner(processorConfig);
        ApplicationStatus status = runner.status();
        log.debug("Status is {} for processor {}", new Object[]{status, processor.getId()});

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

  private ProcessorGraph getExecutionPlan(StreamApplication app) throws Exception {
    // build stream graph
    StreamGraph streamGraph = new StreamGraphImpl(this, config);
    app.init(streamGraph, config);

    // create the physical execution plan
    ExecutionPlanner planner = new ExecutionPlanner(config);
    return planner.plan(streamGraph);
  }
}
