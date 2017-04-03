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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.SamzaException;
import org.apache.samza.execution.ExecutionPlanner;
import org.apache.samza.execution.JobGraph;
import org.apache.samza.job.JobRunner;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraphImpl;
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
  @Override public void run(StreamApplication app) {
    try {
      // 1. build stream graph
      StreamGraph streamGraph = new StreamGraphImpl(this, config);
      app.init(streamGraph, config);

      // 2. create the physical execution plan
      ExecutionPlanner planner = new ExecutionPlanner(config);
      JobGraph jobGraph = planner.plan(streamGraph);

      // 3. submit jobs for remote execution
      jobGraph.getProcessorNodes().forEach(processor -> {
          Config processorConfig = processor.generateConfig();
          log.info("Starting processor {} with config {}", processor.getId(), config);
          JobRunner runner = new JobRunner(processorConfig);
          runner.run(true);
        });
    } catch (Throwable t) {
      throw new SamzaException("Fail to run application", t);
    }
  }
}
