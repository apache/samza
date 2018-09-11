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

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JobNode is a physical execution unit. In RemoteExecutionEnvironment, it's a job that will be submitted
 * to remote cluster. In LocalExecutionEnvironment, it's a set of StreamProcessors for local execution.
 * A JobNode contains the input/output, and the configs for physical execution.
 */
public class JobNode {
  private static final Logger log = LoggerFactory.getLogger(JobNode.class);

  private final String jobName;
  private final String jobId;
  private final String id;
  private final Config config;
  private final JobGraphConfigureGenerator configGenerator;
  private final List<StreamEdge> inEdges = new ArrayList<>();
  private final List<StreamEdge> outEdges = new ArrayList<>();
  private final List<TableSpec> tables = new ArrayList<>();

  JobNode(String jobName, String jobId, Config config, JobGraphConfigureGenerator configureGenerator) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.id = createId(jobName, jobId);
    this.config = config;
    this.configGenerator = configureGenerator;
  }

  String getId() {
    return id;
  }

  String getJobName() {
    return jobName;
  }

  String getJobId() {
    return jobId;
  }

  Config getConfig() {
    return config;
  }

  void addInEdge(StreamEdge in) {
    inEdges.add(in);
  }

  void addOutEdge(StreamEdge out) {
    outEdges.add(out);
  }

  void addTable(TableSpec tableSpec) {
    tables.add(tableSpec);
  }

  List<StreamEdge> getInEdges() {
    return inEdges;
  }

  List<StreamEdge> getOutEdges() {
    return outEdges;
  }

  List<TableSpec> getTables() {
    return tables;
  }

  /**
   * Generate the configs for a job
   * @param executionPlanJson JSON representation of the execution plan
   * @return config of the job
   */
  JobConfig generateConfig(String executionPlanJson) {
    return configGenerator.generateJobConfig(this, executionPlanJson);
  }

  static String createId(String jobName, String jobId) {
    return String.format("%s-%s", jobName, jobId);
  }

}
