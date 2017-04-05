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

package org.apache.samza.execution;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.StreamSpec;


/**
 * This class represents Samza {@link org.apache.samza.application.StreamApplication}
 * plans for physical execution.
 */
public class StreamPlan {
  private final StreamGraph streamGraph;
  private final JobGraph jobGraph;
  private final PlanJsonGenerator jsonGenerator = new PlanJsonGenerator();

  public StreamPlan(StreamGraph streamGraph, JobGraph jobGraph) {
    this.streamGraph = streamGraph;
    this.jobGraph = jobGraph;
  }

  /**
   * Returns the final {@link StreamGraph} after optimization and plan.
   * @return {@link StreamGraph}
   */
  public StreamGraph getStreamGraph() {
    return streamGraph;
  }

  /**
   * Returns the configs for single stage job, in the order of topologically sort.
   * @return list of job configs
   */
  public List<JobConfig> getJobConfigs() {
    return jobGraph.getJobNodes().stream().map(JobNode::generateConfig).collect(Collectors.toList());
  }

  /**
   * Returns the intermediate streams that need to be created.
   * @return intermediate {@link StreamSpec}s
   */
  public List<StreamSpec> getIntermediateStreams() {
    return jobGraph.getIntermediateStreams().stream()
        .map(streamEdge -> streamEdge.getStreamSpec())
        .collect(Collectors.toList());
  }

  /**
   * Returns the JSON representation of the plan for visualization
   * @return json string
   * @throws Exception
   */
  public String getPlanAsJson() throws Exception {
    return jsonGenerator.toJson(jobGraph);
  }
}
