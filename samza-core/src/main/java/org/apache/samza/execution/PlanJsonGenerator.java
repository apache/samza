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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This class generates the json representation of the plan.
 */
public class PlanJsonGenerator {

  private static abstract class GraphNode {
    private Integer nodeId;
    private Set<Integer> successors = new HashSet<>();

    public void setSuccessors(Set<Integer> successors) {
      this.successors = successors;
    }

    public void setNodeId(Integer nodeId) {
      this.nodeId = nodeId;
    }

    public Integer getNodeId() {
      return nodeId;
    }

    public Set<Integer> getSuccessors() {
      return successors;
    }
  }

  static final class OpNode extends GraphNode {
    private String opCode;
    private int opId;
    private int pairNodeId = -1;  //for join operator, we will have a pair nodes for two partial joins

    public int getPairNodeId() {
      return pairNodeId;
    }

    public void setPairNodeId(int pairNodeId) {
      this.pairNodeId = pairNodeId;
    }

    public String getOpCode() {
      return opCode;
    }

    public void setOpCode(String opCode) {
      this.opCode = opCode;
    }

    public int getOpId() {
      return opId;
    }

    public void setOpId(int opId) {
      this.opId = opId;
    }
  }

  static final class StreamNode extends GraphNode {
    private String streamId;
    private String system;
    private String physicalName;
    private int partition;

    public String getStreamId() {
      return streamId;
    }

    public void setStreamId(String streamId) {
      this.streamId = streamId;
    }

    public String getSystem() {
      return system;
    }

    public void setSystem(String system) {
      this.system = system;
    }

    public String getPhysicalName() {
      return physicalName;
    }

    public void setPhysicalName(String stream) {
      this.physicalName = stream;
    }

    public int getPartition() {
      return partition;
    }

    public void setPartition(int partition) {
      this.partition = partition;
    }
  }

  static final class Job {
    private String jobName;
    private String jobId;
    private List<OpNode> opNodes;

    public String getJobName() {
      return jobName;
    }

    public void setJobName(String jobName) {
      this.jobName = jobName;
    }

    public String getJobId() {
      return jobId;
    }

    public void setJobId(String jobId) {
      this.jobId = jobId;
    }

    public List<OpNode> getOpNodes() {
      return opNodes;
    }

    public void setOpNodes(List<OpNode> opNodes) {
      this.opNodes = opNodes;
    }
  }

  static final class GraphNodes {
    private List<Job> jobs;
    private List<StreamNode> streams;

    public List<Job> getJobs() {
      return jobs;
    }

    public void setJobs(List<Job> jobs) {
      this.jobs = jobs;
    }

    public List<StreamNode> getStreams() {
      return streams;
    }

    public void setStreams(List<StreamNode> streams) {
      this.streams = streams;
    }
  }

  private int nextID = 0;
  // streamId to StreamNode mapping
  private final Map<String, StreamNode> streamNodes = new HashMap<>();
  // Mapping from the output stream to the join spec. Since StreamGraph creates two partial join operators for a join and they
  // will have the same output stream, this mapping is used to choose one of them as the unique join spec representing this join
  // (who register first in the map wins).
  Map<MessageStream, OperatorSpec> outputStreamToJoinSpec = new HashMap<>();

  /* package private */ String toJson(JobGraph jobGraph) throws Exception {
    StreamGraphImpl streamGraph = (StreamGraphImpl) jobGraph.getStreamGraph();
    GraphNodes nodes = new GraphNodes();
    List<Job> jobs = jobGraph.getJobNodes().stream()
        .map(jobNode -> buildJob(jobNode, streamGraph, jobGraph))
        .collect(Collectors.toList());
    nodes.setJobs(jobs);
    nodes.setStreams(createListAndSort(streamNodes.values()));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(out, nodes);
    return new String(out.toByteArray());
  }

  private Job buildJob(JobNode jobNode, StreamGraphImpl streamGraph, JobGraph jobGraph) {
    Map<OutputStream, StreamEdge> outputToStreamSpecs = streamGraph.getOutStreams().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getValue, e -> jobGraph.getOrCreateEdge(e.getKey())));
    Map<Integer, OpNode> opNodes = new HashMap<>();

    streamGraph.getInStreams().forEach((streamSpec, stream) -> {
        StreamEdge streamEdge = jobGraph.getOrCreateEdge(streamSpec);
        GraphNode node = getOrCreateStreamNode(streamEdge);
        buildJsonGraph(stream, node, opNodes, outputToStreamSpecs);
      });

    Job job = new Job();
    job.setJobName(jobNode.getJobName());
    job.setJobId(jobNode.getJobId());
    job.setOpNodes(createListAndSort(opNodes.values()));
    return job;
  }

  private void buildJsonGraph(MessageStream inputStream, GraphNode node, Map<Integer, OpNode> opNodes, Map<OutputStream, StreamEdge> outputToStreamSpecs) {
    Collection<OperatorSpec> specs = ((MessageStreamImpl) inputStream).getRegisteredOperatorSpecs();
    for (OperatorSpec spec : specs) {
      GraphNode next = getOrCreateOpNode(opNodes, spec);
      node.getSuccessors().add(next.getNodeId());

      if (spec instanceof SinkOperatorSpec) {
        StreamEdge streamEdge = outputToStreamSpecs.get(((SinkOperatorSpec) spec).getOutStream());
        GraphNode streamNode = getOrCreateStreamNode(streamEdge);
        next.getSuccessors().add(streamNode.getNodeId());
        next = streamNode;
      }

      if (spec.getNextStream() != null) {
        buildJsonGraph(spec.getNextStream(), next, opNodes, outputToStreamSpecs);
      }
    }
  }

  private OpNode getOrCreateOpNode(Map<Integer, OpNode> opNodes, OperatorSpec opSpec) {
    OpNode node = opNodes.get(opSpec.getOpId());
    if (node == null) {
      node = new OpNode();
      node.setNodeId(nextID++);
      node.setOpCode(opSpec.getOpCode().name());
      node.setOpId(opSpec.getOpId());
      opNodes.put(opSpec.getOpId(), node);
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
      } else {
        OpNode joinNode = opNodes.get(joinSpec.getOpId());
        joinNode.setPairNodeId(node.getNodeId());
        node.setPairNodeId(joinNode.getNodeId());
      }
    }

    return node;
  }

  private StreamNode getOrCreateStreamNode(StreamEdge edge) {
    String streamId = edge.getStreamSpec().getId();
    StreamNode node = streamNodes.get(streamId);
    if (node == null) {
      node = new StreamNode();
      node.setNodeId(nextID++);
      node.setStreamId(streamId);
      node.setSystem(edge.getStreamSpec().getSystemName());
      node.setPhysicalName(edge.getStreamSpec().getPhysicalName());
      node.setPartition(edge.getPartitionCount());
      streamNodes.put(streamId, node);
    }
    return node;
  }

  private static <T extends GraphNode> List<T> createListAndSort(Collection<T> nodes) {
    List<T> list = new ArrayList<>(nodes);
    Collections.sort(list, (o1, o2) -> o1.getNodeId().compareTo(o2.getNodeId()));
    return list;
  }
}
