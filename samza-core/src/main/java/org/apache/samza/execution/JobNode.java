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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.application.ApplicationDescriptor;
import org.apache.samza.application.ApplicationDescriptorImpl;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.serializers.Serde;
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
  private final JobNodeConfigureGenerator configGenerator;
  private final Map<String, StreamEdge> inEdges = new HashMap<>();
  private final Map<String, StreamEdge> outEdges = new HashMap<>();
  private final Map<String, TableSpec> tables = new HashMap<>();
  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;

  JobNode(String jobName, String jobId, Config config, ApplicationDescriptorImpl appDesc,
      JobNodeConfigureGenerator configureGenerator) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.id = createId(jobName, jobId);
    this.config = config;
    this.appDesc = appDesc;
    this.configGenerator = configureGenerator;
  }

  static String createId(String jobName, String jobId) {
    return String.format("%s-%s", jobName, jobId);
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
    inEdges.put(in.getStreamSpec().getId(), in);
  }

  void addOutEdge(StreamEdge out) {
    outEdges.put(out.getStreamSpec().getId(), out);
  }

  void addTable(TableSpec tableSpec) {
    tables.put(tableSpec.getId(), tableSpec);
  }

  Map<String, StreamEdge> getInEdges() {
    return inEdges;
  }

  Map<String, StreamEdge> getOutEdges() {
    return outEdges;
  }

  Map<String, TableSpec> getTables() {
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

  Serde getInputSerde(String streamId) {
    return appDesc.getInputSerde(streamId);
  }

  Serde getOutputSerde(String streamId) {
    return appDesc.getOutputSerde(streamId);
  }

  Collection<OperatorSpec> getReachableOperators() {
    Set<OperatorSpec> inputOperatorsInJobNode = inEdges.values().stream().map(inEdge ->
        appDesc.getInputOperators().get(inEdge.getStreamSpec().getId())).filter(Objects::nonNull).collect(Collectors.toSet());
    Set<OperatorSpec> reachableOperators = new HashSet<>();
    findReachableOperators(inputOperatorsInJobNode, reachableOperators);
    return reachableOperators;
  }

  private void findReachableOperators(Collection<OperatorSpec> inputOperatorsInJobNode,
      Set<OperatorSpec> reachableOperators) {
    inputOperatorsInJobNode.forEach(op -> {
        if (reachableOperators.contains(op)) {
          return;
        }
        reachableOperators.add(op);
        findReachableOperators(op.getRegisteredOperatorSpecs(), reachableOperators);
      });
  }

  // get all next operators consuming from the input {@code streamId}
  Set<String> getNextOperatorIds(String streamId) {
    if (!appDesc.getInputOperators().containsKey(streamId) || !inEdges.containsKey(streamId)) {
      return new HashSet<>();
    }
    return appDesc.getInputOperators().get(streamId).getRegisteredOperatorSpecs().stream()
        .map(op -> op.getOpId()).collect(Collectors.toSet());
  }

  InputOperatorSpec getInputOperator(String inputStreamId) {
    if (!inEdges.containsKey(inputStreamId)) {
      return null;
    }
    return appDesc.getInputOperators().get(inputStreamId);
  }

  boolean isLegacyTaskApplication() {
    return ApplicationUtil.isLegacyTaskApplication(appDesc);
  }
}
