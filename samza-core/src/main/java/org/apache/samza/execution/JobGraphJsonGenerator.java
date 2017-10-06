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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.TableSpec;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class generates the JSON representation of the {@link JobGraph}.
 */
/* package private */ class JobGraphJsonGenerator {

  static final class StreamSpecJson {
    @JsonProperty("id")
    String id;
    @JsonProperty("systemName")
    String systemName;
    @JsonProperty("physicalName")
    String physicalName;
    @JsonProperty("partitionCount")
    int partitionCount;
  }

  static final class TableSpecJson {
    @JsonProperty("id")
    String id;
    @JsonProperty("tableProviderFactory")
    String tableProviderFactory;
    @JsonProperty("config")
    Map<String, String> config;
  }

  static final class StreamEdgeJson {
    @JsonProperty("streamSpec")
    StreamSpecJson streamSpec;
    @JsonProperty("sourceJobs")
    List<String> sourceJobs;
    @JsonProperty("targetJobs")
    List<String> targetJobs;
  }

  static final class OperatorGraphJson {
    @JsonProperty("inputStreams")
    List<StreamJson> inputStreams;
    @JsonProperty("outputStreams")
    List<StreamJson> outputStreams;
    @JsonProperty("operators")
    Map<Integer, Map<String, Object>> operators = new HashMap<>();
  }

  static final class StreamJson {
    @JsonProperty("streamId")
    String streamId;
    @JsonProperty("nextOperatorIds")
    Set<Integer>  nextOperatorIds = new HashSet<>();
  }

  static final class JobNodeJson {
    @JsonProperty("jobName")
    String jobName;
    @JsonProperty("jobId")
    String jobId;
    @JsonProperty("operatorGraph")
    OperatorGraphJson operatorGraph;
  }

  static final class JobGraphJson {
    @JsonProperty("jobs")
    List<JobNodeJson> jobs;
    @JsonProperty("sourceStreams")
    Map<String, StreamEdgeJson> sourceStreams;
    @JsonProperty("sinkStreams")
    Map<String, StreamEdgeJson> sinkStreams;
    @JsonProperty("intermediateStreams")
    Map<String, StreamEdgeJson> intermediateStreams;
    @JsonProperty("tables")
    Map<String, TableSpecJson> tables;
    @JsonProperty("applicationName")
    String applicationName;
    @JsonProperty("applicationId")
    String applicationId;
  }

  /**
   * Returns the JSON representation of a {@link JobGraph}
   * @param jobGraph {@link JobGraph}
   * @return JSON of the graph
   * @throws Exception exception during creating JSON
   */
  /* package private */ String toJson(JobGraph jobGraph) throws Exception {
    JobGraphJson jobGraphJson = new JobGraphJson();

    // build StreamEdge JSON
    ApplicationConfig appConfig = jobGraph.getApplicationConfig();
    jobGraphJson.applicationName = appConfig.getAppName();
    jobGraphJson.applicationId = appConfig.getAppId();
    jobGraphJson.sourceStreams = new HashMap<>();
    jobGraphJson.sinkStreams = new HashMap<>();
    jobGraphJson.intermediateStreams = new HashMap<>();
    jobGraphJson.tables = new HashMap<>();
    jobGraph.getSources().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.sourceStreams));
    jobGraph.getSinks().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.sinkStreams));
    jobGraph.getIntermediateStreamEdges().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.intermediateStreams));
    jobGraph.getTables().forEach(t -> buildTableJson(t, jobGraphJson.tables));

    jobGraphJson.jobs = jobGraph.getJobNodes().stream()
        .map(jobNode -> buildJobNodeJson(jobNode))
        .collect(Collectors.toList());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(out, jobGraphJson);
    return new String(out.toByteArray());
  }

  /**
   * Create JSON POJO for a {@link JobNode}, including the {@link org.apache.samza.operators.StreamGraph} for this job
   * @param jobNode job node in the {@link JobGraph}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.JobNodeJson}
   */
  private JobNodeJson buildJobNodeJson(JobNode jobNode) {
    JobNodeJson job = new JobNodeJson();
    job.jobName = jobNode.getJobName();
    job.jobId = jobNode.getJobId();
    job.operatorGraph = buildOperatorGraphJson(jobNode);
    return job;
  }

  /**
   * Traverse the {@link OperatorSpec} graph and build the operator graph JSON POJO.
   * @param jobNode job node in the {@link JobGraph}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.OperatorGraphJson}
   */
  private OperatorGraphJson buildOperatorGraphJson(JobNode jobNode) {
    OperatorGraphJson opGraph = new OperatorGraphJson();
    opGraph.inputStreams = new ArrayList<>();
    jobNode.getStreamGraph().getInputOperators().forEach((streamSpec, operatorSpec) -> {
        StreamJson inputJson = new StreamJson();
        opGraph.inputStreams.add(inputJson);
        inputJson.streamId = streamSpec.getId();
        Collection<OperatorSpec> specs = operatorSpec.getRegisteredOperatorSpecs();
        inputJson.nextOperatorIds = specs.stream().map(OperatorSpec::getOpId).collect(Collectors.toSet());

        updateOperatorGraphJson(operatorSpec, opGraph);
      });

    opGraph.outputStreams = new ArrayList<>();
    jobNode.getStreamGraph().getOutputStreams().keySet().forEach(streamSpec -> {
        StreamJson outputJson = new StreamJson();
        outputJson.streamId = streamSpec.getId();
        opGraph.outputStreams.add(outputJson);
      });
    return opGraph;
  }

  /**
   * Traverse the {@link OperatorSpec} graph recursively and update the operator graph JSON POJO.
   * @param operatorSpec input
   * @param opGraph operator graph to build
   */
  private void updateOperatorGraphJson(OperatorSpec operatorSpec, OperatorGraphJson opGraph) {
    // TODO xiliu: render input operators instead of input streams
    if (operatorSpec.getOpCode() != OpCode.INPUT) {
      opGraph.operators.put(operatorSpec.getOpId(), operatorToMap(operatorSpec));
    }
    Collection<OperatorSpec> specs = operatorSpec.getRegisteredOperatorSpecs();
    specs.forEach(opSpec -> updateOperatorGraphJson(opSpec, opGraph));
  }

  /**
   * Format the operator properties into a map
   * @param spec a {@link OperatorSpec} instance
   * @return map of the operator properties
   */
  private Map<String, Object> operatorToMap(OperatorSpec spec) {
    Map<String, Object> map = new HashMap<>();
    map.put("opCode", spec.getOpCode().name());
    map.put("opId", spec.getOpId());
    map.put("sourceLocation", spec.getSourceLocation());

    Collection<OperatorSpec> nextOperators = spec.getRegisteredOperatorSpecs();
    map.put("nextOperatorIds", nextOperators.stream().map(OperatorSpec::getOpId).collect(Collectors.toSet()));

    if (spec instanceof OutputOperatorSpec) {
      OutputStreamImpl outputStream = ((OutputOperatorSpec) spec).getOutputStream();
      map.put("outputStreamId", outputStream.getStreamSpec().getId());
    }

    if (spec instanceof StreamTableJoinOperatorSpec) {
      TableSpec tableSpec = ((StreamTableJoinOperatorSpec) spec).getTableSpec();
      map.put("tableId", tableSpec.getId());
    }

    if (spec instanceof JoinOperatorSpec) {
      map.put("ttlMs", ((JoinOperatorSpec) spec).getTtlMs());
    }

    return map;
  }

  /**
   * Get or create the JSON POJO for a {@link StreamEdge}
   * @param edge {@link StreamEdge}
   * @param streamEdges map of streamId to {@link org.apache.samza.execution.JobGraphJsonGenerator.StreamEdgeJson}
   * @return JSON representation of the {@link StreamEdge}
   */
  private StreamEdgeJson buildStreamEdgeJson(StreamEdge edge, Map<String, StreamEdgeJson> streamEdges) {
    String streamId = edge.getStreamSpec().getId();
    StreamEdgeJson edgeJson = streamEdges.get(streamId);
    if (edgeJson == null) {
      edgeJson = new StreamEdgeJson();
      StreamSpecJson streamSpecJson = new StreamSpecJson();
      streamSpecJson.id = streamId;
      streamSpecJson.systemName = edge.getStreamSpec().getSystemName();
      streamSpecJson.physicalName = edge.getStreamSpec().getPhysicalName();
      streamSpecJson.partitionCount = edge.getPartitionCount();
      edgeJson.streamSpec = streamSpecJson;

      List<String> sourceJobs = new ArrayList<>();
      edge.getSourceNodes().forEach(jobNode -> {
          sourceJobs.add(jobNode.getJobName());
        });
      edgeJson.sourceJobs = sourceJobs;

      List<String> targetJobs = new ArrayList<>();
      edge.getTargetNodes().forEach(jobNode -> {
          targetJobs.add(jobNode.getJobName());
        });
      edgeJson.targetJobs = targetJobs;

      streamEdges.put(streamId, edgeJson);
    }
    return edgeJson;
  }

  private TableSpecJson buildTableJson(TableSpec tableSpec, Map<String, TableSpecJson> tableSpecs) {
    String tableId = tableSpec.getId();
    TableSpecJson tableSpecJson = tableSpecs.get(tableId);
    if (tableSpecJson == null) {
      tableSpecJson = buildTableJson(tableSpec);
      tableSpecs.put(tableId, tableSpecJson);
    }
    return tableSpecJson;
  }

  /**
   * Get or create the JSON POJO for a {@link TableSpec}
   * @param tableSpec the tableSpec
   * @return JSON representation of the {@link TableSpec}
   */
  public TableSpecJson buildTableJson(TableSpec tableSpec) {
    TableSpecJson tableSpecJson = new TableSpecJson();
    tableSpecJson.id = tableSpec.getId();
    tableSpecJson.tableProviderFactory = tableSpec.getTableProviderFactory();
    tableSpecJson.config = tableSpec.getConfig();
    return tableSpecJson;
  }
}
