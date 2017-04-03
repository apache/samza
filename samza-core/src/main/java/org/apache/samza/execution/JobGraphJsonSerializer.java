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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;

public class JobGraphJsonSerializer {

  private static abstract class GraphNode {
    private static int NEXT_ID = 0;
    private final Integer nodeId;

    public GraphNode() {
      nodeId = NEXT_ID++;
    }

    public Integer getNodeId() {
      return nodeId;
    }
  }

  private static final class JobNode extends GraphNode {
    private String jobName;
    private String jobId;
    private String opName;
    private int opId;
    private List<Integer> successors;

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

    public String getOpName() {
      return opName;
    }

    public void setOpName(String opName) {
      this.opName = opName;
    }

    public int getOpId() {
      return opId;
    }

    public void setOpId(int opId) {
      this.opId = opId;
    }

    public List<Integer> getSuccessors() {
      return successors;
    }

    public void setSuccessors(List<Integer> successors) {
      this.successors = successors;
    }
  }

  private static final class StreamNode extends GraphNode {
    private String streamId;
    private String system;
    private String stream;
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

    public String getStream() {
      return stream;
    }

    public void setStream(String stream) {
      this.stream = stream;
    }

    public int getPartition() {
      return partition;
    }

    public void setPartition(int partition) {
      this.partition = partition;
    }
  }

  // operatorId to JobNode mapping
  private final Map<Integer, JobNode> jobNodes = new HashMap<>();
  // streamId to StreamNode mapping
  private final Map<String, StreamNode> streamNodes = new HashMap<>();

  String toJson(JobGraph graph) {
    StreamGraphImpl streamGraph = (StreamGraphImpl) graph.getStreamGraph();
    return "";
  }

  private JobNode getOrCreateJobNode(OperatorSpec opSpec) {
    JobNode node = jobNodes.get(opSpec.getOpId());
    if (node == null) {
      node = new JobNode();
    }

    return node;
  }


}
