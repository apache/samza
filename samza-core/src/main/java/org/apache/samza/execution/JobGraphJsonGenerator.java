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
public class JobGraphJsonGenerator {

  /**
   * This class provides the necessary connection of operators for traversal.
   */
  static abstract class Traversable {
    @JsonProperty("NextOperatorIds")
    Set<Integer>  nextOperatorIds = new HashSet<>();
  }

  static final class OperatorJson extends Traversable {
    @JsonProperty("OpCode")
    String opCode;
    @JsonProperty("OpId")
    int opId;
    @JsonProperty("OutputStreamId")
    String outputStreamId;
    @JsonProperty("PairedOpId")
    int pairedOpId = -1;  //for join operator, we will have a pair nodes for two partial joins
  }

  static final class StreamSpecJson {
    @JsonProperty("Id")
    String id;
    @JsonProperty("SystemName")
    String systemName;
    @JsonProperty("PhysicalName")
    String physicalName;
    @JsonProperty("PartitionCount")
    int partitionCount;
  }

  static final class StreamEdgeJson {
    @JsonProperty("StreamSpec")
    StreamSpecJson streamSpec;
  }

  static final class OperatorGraphJson {
    @JsonProperty("InputStreams")
    List<InputStreamJson> inputStreams;
    @JsonProperty("Operators")
    Map<Integer, OperatorJson> operators = new HashMap<>();
  }

  static final class InputStreamJson extends Traversable {
    @JsonProperty("StreamId")
    String streamId;
  }

  static final class JobNodeJson {
    @JsonProperty("JobName")
    String jobName;
    @JsonProperty("JobId")
    String jobId;
    @JsonProperty("OperatorGraph")
    OperatorGraphJson operatorGraph;
  }

  static final class JobGraphJson {
    @JsonProperty("Jobs")
    List<JobNodeJson> jobs;
    @JsonProperty("Streams")
    Map<String, StreamEdgeJson> streams;
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
    jobGraphJson.streams = new HashMap<>();
    jobGraph.getSources().forEach(e -> getOrCreateStreamEdgeJson(e, jobGraphJson.streams));
    jobGraph.getSinks().forEach(e -> getOrCreateStreamEdgeJson(e, jobGraphJson.streams));
    jobGraph.getIntermediateStreamEdges().forEach(e -> getOrCreateStreamEdgeJson(e, jobGraphJson.streams));

    jobGraphJson.jobs = jobGraph.getJobNodes().stream()
        .map(jobNode -> buildJobNodeJson(jobNode, jobGraphJson.streams))
        .collect(Collectors.toList());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(out, jobGraphJson);
    return new String(out.toByteArray());
  }

  /**
   * Create JSON POJO for a {@link JobNode}, including the {@link org.apache.samza.operators.StreamGraph} for this job
   * @param jobNode job node in the {@link JobGraph}
   * @param streamEdges map of {@link org.apache.samza.execution.JobGraphJsonGenerator.StreamEdgeJson}
   * @return {@link org.apache.samza.execution.JobGraphJsonGenerator.JobNodeJson}
   */
  private JobNodeJson buildJobNodeJson(JobNode jobNode, Map<String, StreamEdgeJson> streamEdges) {
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

        OperatorJson opJson = getOrCreateOperatorJson(opSpec, opGraph);
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
  private OperatorJson getOrCreateOperatorJson(OperatorSpec opSpec, OperatorGraphJson opGraph) {
    Map<Integer, OperatorJson> operators = opGraph.operators;
    OperatorJson opJson = operators.get(opSpec.getOpId());
    if (opJson == null) {
      opJson = new OperatorJson();
      opJson.opCode = opSpec.getOpCode().name();
      opJson.opId = opSpec.getOpId();
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
  private StreamEdgeJson getOrCreateStreamEdgeJson(StreamEdge edge, Map<String, StreamEdgeJson> streamEdges) {
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
      streamEdges.put(streamId, edgeJson);
    }
    return edgeJson;
  }
}
