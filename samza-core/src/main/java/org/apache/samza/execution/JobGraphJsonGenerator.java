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
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This class generates the JSON representation of the {@link JobGraph}.
 */
/* package private */ class JobGraphJsonGenerator {

  /**
   * This class provides the necessary connection of operators for traversal.
   */
  static abstract class Traversable {
    @JsonProperty("nextOperatorIds")
    Set<Integer>  nextOperatorIds = new HashSet<>();
  }

  static final class OperatorJson extends Traversable {
    @JsonProperty("opCode")
    String opCode;
    @JsonProperty("opId")
    int opId;
    @JsonProperty("outputStreamId")
    String outputStreamId;
    @JsonProperty("pairedOpId")
    int pairedOpId = -1;  //for join operator, we will have a pair nodes for two partial joins
    @JsonProperty("caller")
    String caller;
  }

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
    List<InputStreamJson> inputStreams;
    @JsonProperty("operators")
    Map<Integer, OperatorJson> operators = new HashMap<>();
    @JsonProperty("outputStreams")
    List<String> outputStreams;
  }

  static final class InputStreamJson extends Traversable {
    @JsonProperty("streamId")
    String streamId;
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
    @JsonProperty("applicationName")
    String applicationName;
    @JsonProperty("applicationId")
    String applicationId;
  }

  // Mapping from the output stream to the join spec. Since StreamGraph creates two partial join operators for a join and they
  // will have the same output stream, this mapping is used to choose one of them as the unique join spec representing this join
  // (who register first in the map wins).
  Map<MessageStream, OperatorSpec> outputStreamToJoinSpec = new HashMap<>();

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
    jobGraph.getSources().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.sourceStreams));
    jobGraph.getSinks().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.sinkStreams));
    jobGraph.getIntermediateStreamEdges().forEach(e -> buildStreamEdgeJson(e, jobGraphJson.intermediateStreams));

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
   * Traverse the {@StreamGraph} and build the operator graph JSON POJO.
   * @param jobNode job node in the {@link JobGraph}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.OperatorGraphJson}
   */
  private OperatorGraphJson buildOperatorGraphJson(JobNode jobNode) {
    OperatorGraphJson opGraph = new OperatorGraphJson();
    opGraph.inputStreams = new ArrayList<>();
    jobNode.getStreamGraph().getInputStreams().forEach((streamSpec, stream) -> {
        InputStreamJson inputJson = new InputStreamJson();
        inputJson.streamId = streamSpec.getId();
        opGraph.inputStreams.add(inputJson);
        updateOperatorGraphJson((MessageStreamImpl) stream, inputJson, opGraph);
      });
    opGraph.outputStreams = new ArrayList<>();
    jobNode.getStreamGraph().getOutputStreams().keySet().forEach(streamSpec -> {
        opGraph.outputStreams.add(streamSpec.getId());
      });
    return opGraph;
  }

  /**
   * Traverse the {@StreamGraph} recursively and update the operator graph JSON POJO.
   * @param messageStream input
   * @param parent parent node in the traveral
   * @param opGraph operator graph to build
   */
  private void updateOperatorGraphJson(MessageStreamImpl messageStream, Traversable parent, OperatorGraphJson opGraph) {
    Collection<OperatorSpec> specs = messageStream.getRegisteredOperatorSpecs();
    specs.forEach(opSpec -> {
        parent.nextOperatorIds.add(opSpec.getOpId());

        OperatorJson opJson = buildOperatorJson(opSpec, opGraph, messageStream);
        if (opSpec instanceof SinkOperatorSpec) {
          opJson.outputStreamId = ((SinkOperatorSpec) opSpec).getOutputStream().getStreamSpec().getId();
        } else if (opSpec.getNextStream() != null) {
          updateOperatorGraphJson(opSpec.getNextStream(), opJson, opGraph);
        }
      });
  }

  /**
   * Get or create the JSON POJO for an operator.
   * @param opSpec {@link OperatorSpec}
   * @param opGraph {@link org.apache.samza.execution.JobGraphJsonGenerator.OperatorGraphJson}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.OperatorJson}
   */
  private OperatorJson buildOperatorJson(OperatorSpec opSpec, OperatorGraphJson opGraph,
      MessageStreamImpl messageStream) {
    Map<Integer, OperatorJson> operators = opGraph.operators;
    OperatorJson opJson = operators.get(opSpec.getOpId());
    if (opJson == null) {
      opJson = new OperatorJson();
      opJson.opCode = opSpec.getOpCode().name();
      opJson.opId = opSpec.getOpId();
      StackTraceElement stackTraceElement = messageStream.getCallerStackTrace(opSpec.getOpId());
      if (stackTraceElement != null) {
        opJson.caller = String.format("%s:%s", stackTraceElement.getFileName(), stackTraceElement.getLineNumber());
      }
      operators.put(opSpec.getOpId(), opJson);
    }

    if (opSpec instanceof PartialJoinOperatorSpec) {
      // every join will have two partial join operators
      // we will choose one of them in order to consolidate the inputs
      // the first one who registered with the outputStreamToJoinSpec will win
      MessageStream output = opSpec.getNextStream();
      OperatorSpec joinSpec = outputStreamToJoinSpec.get(output);
      if (joinSpec == null) {
        joinSpec = opSpec;
        outputStreamToJoinSpec.put(output, joinSpec);
      } else if (joinSpec != opSpec) {
        OperatorJson joinNode = operators.get(joinSpec.getOpId());
        joinNode.pairedOpId = opJson.opId;
        opJson.pairedOpId = joinNode.opId;
      }
    }

    return opJson;
  }

  /**
   * Get or create the JSON POJO for a {@link StreamEdge}
   * @param edge {@link StreamEdge}
   * @param streamEdges map of streamId to {@link org.apache.samza.execution.JobGraphJsonGenerator.StreamEdgeJson}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.StreamEdgeJson}
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
}
