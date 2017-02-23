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
package org.apache.samza.system;

import org.apache.samza.SamzaException;
import org.apache.samza.job.JobRunner;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.processorgraph.ExecutionPlanner;
import org.apache.samza.processorgraph.ProcessorGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ExecutionEnvironment} that runs the applications in YARN environment
 */
public class RemoteExecutionEnvironment extends AbstractExecutionEnvironment {

  public RemoteExecutionEnvironment(Config config) {
    super(config);
  }
  private static final Logger log = LoggerFactory.getLogger(RemoteExecutionEnvironment.class);

  @Override public void run(StreamGraphBuilder app, Config config) {
    // TODO: add description of ProcessContext that is going to create a sub-DAG of the {@code graph}
    // TODO: actually instantiate the tasks and run the job, i.e.
    try {
      // 1. build stream graph
      StreamGraph streamGraph = new StreamGraphImpl();
      app.init(streamGraph, config);

      // 2. create the physical execution plan
      ExecutionPlanner planner = new ExecutionPlanner(config);
      ProcessorGraph processorGraph = planner.plan(streamGraph);

      // 3. submit jobs for remote execution
      processorGraph.getProcessors().forEach(processor -> {
          Config processorConfig = processor.generateConfig();
          String processorId = processor.getId();
          log.info("Starting processor {} with config {}", processorId, config);

          JobRunner runner = new JobRunner(processorConfig);
          runner.run(true);
        });
    } catch (Exception e) {
      throw new SamzaException("fail to run graph", e);
    }
  }
}
