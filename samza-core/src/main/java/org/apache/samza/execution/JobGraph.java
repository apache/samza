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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The JobGraph is the physical execution graph for a multi-stage Samza application.
 * It contains the topology of jobs connected with source/sink/intermediate streams.
 * High level APIs are transformed into JobGraph for planning, validation and execution.
 * Source/sink streams are external streams while intermediate streams are created and managed by Samza.
 * Note that intermediate streams are both the input and output of a JobNode in JobGraph.
 * So the graph may have cycles and it's not a DAG.
 */
/* package private */ class JobGraph implements ExecutionPlan {
  private static final Logger log = LoggerFactory.getLogger(JobGraph.class);

  private final Map<String, JobNode> nodes = new HashMap<>();
  private final Map<String, StreamEdge> edges = new HashMap<>();
  private final Set<StreamEdge> inputStreams = new HashSet<>();
  private final Set<StreamEdge> outputStreams = new HashSet<>();
  private final Set<StreamEdge> intermediateStreams = new HashSet<>();
  private final Set<StreamEdge> sideInputStreams = new HashSet<>();
  private final Set<TableSpec> tables = new HashSet<>();
  private final Config config;
  private final JobGraphJsonGenerator jsonGenerator;
  private final JobNodeConfigurationGenerator configGenerator;
  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;

  /**
   * The JobGraph is only constructed by the {@link ExecutionPlanner}.
   *
   * @param config configuration for the application
   * @param appDesc {@link ApplicationDescriptorImpl} describing the application
   */
  JobGraph(Config config, ApplicationDescriptorImpl appDesc) {
    this.config = config;
    this.appDesc = appDesc;
    this.jsonGenerator = new JobGraphJsonGenerator();
    this.configGenerator = new JobNodeConfigurationGenerator();
  }

  @Override
  public List<JobConfig> getJobConfigs() {
    String json = "";
    try {
      json = getPlanAsJson();
    } catch (Exception e) {
      log.warn("Failed to generate plan JSON", e);
    }

    final String planJson = json;
    return getJobNodes().stream().map(n -> n.generateConfig(planJson)).collect(Collectors.toList());
  }

  @Override
  public List<StreamSpec> getIntermediateStreams() {
    return getIntermediateStreamEdges().stream()
        .map(StreamEdge::getStreamSpec)
        .collect(Collectors.toList());
  }

  @Override
  public String getPlanAsJson() throws Exception {
    return jsonGenerator.toJson(this);
  }

  /**
   * Returns the config for this application
   * @return {@link ApplicationConfig}
   */
  @Override
  public ApplicationConfig getApplicationConfig() {
    return new ApplicationConfig(config);
  }

  /**
   * Add a source stream to a {@link JobNode}
   * @param streamSpec input stream
   * @param node the job node that consumes from the streamSpec
   */
  void addInputStream(StreamSpec streamSpec, JobNode node) {
    StreamEdge edge = getOrCreateStreamEdge(streamSpec);
    edge.addTargetNode(node);
    node.addInEdge(edge);
    inputStreams.add(edge);
  }

  /**
   * Add an output stream to a {@link JobNode}
   * @param streamSpec output stream
   * @param node the job node that outputs to the output stream
   */
  void addOutputStream(StreamSpec streamSpec, JobNode node) {
    StreamEdge edge = getOrCreateStreamEdge(streamSpec);
    edge.addSourceNode(node);
    node.addOutEdge(edge);
    outputStreams.add(edge);
  }

  /**
   * Add an intermediate stream from source to target {@link JobNode}
   * @param streamSpec intermediate stream
   * @param from the source node
   * @param to the target node
   */
  void addIntermediateStream(StreamSpec streamSpec, JobNode from, JobNode to) {
    StreamEdge edge = getOrCreateStreamEdge(streamSpec, true);
    edge.addSourceNode(from);
    edge.addTargetNode(to);
    from.addOutEdge(edge);
    to.addInEdge(edge);
    intermediateStreams.add(edge);
  }

  void addTable(TableSpec tableSpec, JobNode node) {
    tables.add(tableSpec);
    node.addTable(tableSpec);
  }

  /**
   * Add a side-input stream to graph
   * @param streamSpec side-input stream
   */
  void addSideInputStream(StreamSpec streamSpec) {
    StreamEdge edge = getOrCreateStreamEdge(streamSpec, false);
    sideInputStreams.add(edge);
  }

  /**
   * Get the {@link JobNode}. Create one if it does not exist.
   * @param jobName name of the job
   * @param jobId id of the job
   * @return {@link JobNode} created with {@code jobName} and {@code jobId}
   */
  JobNode getOrCreateJobNode(String jobName, String jobId) {
    String nodeId = JobNode.createJobNameAndId(jobName, jobId);
    return nodes.computeIfAbsent(nodeId, k -> new JobNode(jobName, jobId, config, appDesc, configGenerator));
  }

  /**
   * Get the {@link StreamEdge} for a {@link StreamSpec}. Create one if it does not exist.
   * @param streamSpec spec of the StreamEdge
   * @return stream edge
   */
  StreamEdge getOrCreateStreamEdge(StreamSpec streamSpec) {
    return getOrCreateStreamEdge(streamSpec, false);
  }

  /**
   * Returns the {@link ApplicationDescriptorImpl} of this graph.
   * @return Application descriptor implementation
   */
  ApplicationDescriptorImpl<? extends ApplicationDescriptor> getApplicationDescriptorImpl() {
    return appDesc;
  }

  /**
   * Get the {@link StreamEdge} for {@code streamId}.
   *
   * @param streamId the streamId for the {@link StreamEdge}
   * @return stream edge
   */
  StreamEdge getStreamEdge(String streamId) {
    return edges.get(streamId);
  }

  /**
   * Returns the job nodes to be executed in the topological order
   * @return unmodifiable list of {@link JobNode}
   */
  List<JobNode> getJobNodes() {
    List<JobNode> sortedNodes = topologicalSort();
    return Collections.unmodifiableList(sortedNodes);
  }

  /**
   * Returns the input streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getInputStreams() {
    return Collections.unmodifiableSet(inputStreams);
  }

  /**
   * Returns the side-input streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getSideInputStreams() {
    return Collections.unmodifiableSet(sideInputStreams);
  }

  /**
   * Returns the output streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getOutputStreams() {
    return Collections.unmodifiableSet(outputStreams);
  }

  /**
   * Returns the tables in the graph
   * @return unmodifiable set of {@link TableSpec}
   */
  Set<TableSpec> getTables() {
    return Collections.unmodifiableSet(tables);
  }

  /**
   * Returns the intermediate streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getIntermediateStreamEdges() {
    return Collections.unmodifiableSet(intermediateStreams);
  }

  /**
   * Validate the graph has the correct topology, meaning the input streams are coming from external streams,
   * output streams are going to external streams, and the nodes are connected with intermediate streams.
   * Also validate all the nodes are reachable from the input streams.
   */
  void validate() {
    validateInputStreams();
    validateOutputStreams();
    validateInternalStreams();
    validateReachability();
  }

  /**
   * Get the {@link StreamEdge} for a {@link StreamSpec}. Create one if it does not exist.
   * @param streamSpec  spec of the StreamEdge
   * @param isIntermediate  boolean flag indicating whether it's an intermediate stream
   * @return stream edge
   */
  private StreamEdge getOrCreateStreamEdge(StreamSpec streamSpec, boolean isIntermediate) {
    String streamId = streamSpec.getId();
    StreamEdge edge = edges.get(streamId);
    if (edge == null) {
      boolean isBroadcast = appDesc.getBroadcastStreams().contains(streamId);
      edge = new StreamEdge(streamSpec, isIntermediate, isBroadcast, config);
      edges.put(streamId, edge);
    }
    return edge;
  }

  /**
   * Validate the input streams should have indegree being 0 and outdegree greater than 0
   */
  private void validateInputStreams() {
    inputStreams.forEach(edge -> {
        if (!edge.getSourceNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Source stream %s should not have producers.", edge.getName()));
        }
        if (edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Source stream %s should have consumers.", edge.getName()));
        }
      });
  }

  /**
   * Validate the output streams should have outdegree being 0 and indegree greater than 0
   */
  private void validateOutputStreams() {
    outputStreams.forEach(edge -> {
        if (!edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Sink stream %s should not have consumers", edge.getName()));
        }
        if (edge.getSourceNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Sink stream %s should have producers", edge.getName()));
        }
      });
  }

  /**
   * Validate the internal streams should have both indegree and outdegree greater than 0
   */
  private void validateInternalStreams() {
    Set<StreamEdge> internalEdges = new HashSet<>(edges.values());
    internalEdges.removeAll(inputStreams);
    internalEdges.removeAll(sideInputStreams);
    internalEdges.removeAll(outputStreams);

    internalEdges.forEach(edge -> {
        if (edge.getSourceNodes().isEmpty() || edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Internal stream %s should have both producers and consumers", edge.getName()));
        }
      });
  }

  /**
   * Validate all nodes are reachable by input streams.
   */
  private void validateReachability() {
    // validate all nodes are reachable from the input streams
    final Set<JobNode> reachable = findReachable();
    if (reachable.size() != nodes.size()) {
      Set<JobNode> unreachable = new HashSet<>(nodes.values());
      unreachable.removeAll(reachable);
      throw new IllegalArgumentException(String.format("Jobs %s cannot be reached from Sources.",
          String.join(", ", unreachable.stream().map(JobNode::getJobNameAndId).collect(Collectors.toList()))));
    }
  }

  /**
   * Find the reachable set of nodes using BFS.
   * @return reachable set of {@link JobNode}
   */
  Set<JobNode> findReachable() {
    Queue<JobNode> queue = new ArrayDeque<>();
    Set<JobNode> visited = new HashSet<>();

    inputStreams.forEach(input -> {
        List<JobNode> next = input.getTargetNodes();
        queue.addAll(next);
        visited.addAll(next);
      });

    while (!queue.isEmpty()) {
      JobNode node = queue.poll();
      node.getOutEdges().values().stream().flatMap(edge -> edge.getTargetNodes().stream()).forEach(target -> {
          if (!visited.contains(target)) {
            visited.add(target);
            queue.offer(target);
          }
        });
    }

    return visited;
  }

  /**
   * An variation of Kahn's algorithm of topological sorting.
   * This algorithm also takes account of the simple loops in the graph
   * @return topologically sorted {@link JobNode}s
   */
  List<JobNode> topologicalSort() {
    Collection<JobNode> pnodes = nodes.values();
    if (pnodes.size() == 1) {
      return new ArrayList<>(pnodes);
    }

    Queue<JobNode> q = new ArrayDeque<>();
    Map<String, Long> indegree = new HashMap<>();
    Set<JobNode> visited = new HashSet<>();
    pnodes.forEach(node -> {
        String nid = node.getJobNameAndId();
        //only count the degrees of intermediate streams
        long degree = node.getInEdges().values().stream().filter(e -> !inputStreams.contains(e)).count();
        indegree.put(nid, degree);

        if (degree == 0L) {
          // start from the nodes that has no intermediate input streams, so it only consumes from input streams
          q.add(node);
          visited.add(node);
        }
      });

    List<JobNode> sortedNodes = new ArrayList<>();
    Set<JobNode> reachable = new HashSet<>();
    while (sortedNodes.size() < pnodes.size()) {
      // Here we use indegree-based approach to implment Kahn's algorithm for topological sort
      // This approach will not change the graph itself during computation.
      //
      // The algorithm works as:
      // 1. start with nodes with no incoming edges (in degree being 0) and inserted into the list
      // 2. remove the edge from any node in the list to its connected nodes by changing the indegree of the connected nodes.
      // 3. add any new nodes with ingree being 0
      // 4. loop 1-3 until no more nodes with indegree 0
      //
      while (!q.isEmpty()) {
        JobNode node = q.poll();
        sortedNodes.add(node);
        node.getOutEdges().values().stream().flatMap(edge -> edge.getTargetNodes().stream()).forEach(n -> {
            String nid = n.getJobNameAndId();
            Long degree = indegree.get(nid) - 1;
            indegree.put(nid, degree);
            if (degree == 0L && !visited.contains(n)) {
              q.add(n);
              visited.add(n);
            }
            reachable.add(n);
          });
      }

      if (sortedNodes.size() < pnodes.size()) {
        // The remaining nodes have cycles
        // use the following approach to break the cycles
        // start from the nodes that are reachable from previous traverse
        reachable.removeAll(sortedNodes);
        if (!reachable.isEmpty()) {
          //find out the nodes with minimal input edge
          long min = Long.MAX_VALUE;
          JobNode minNode = null;
          for (JobNode node : reachable) {
            Long degree = indegree.get(node.getJobNameAndId());
            if (degree < min) {
              min = degree;
              minNode = node;
            }
          }
          // start from the node with minimal input edge again
          q.add(minNode);
          visited.add(minNode);
        } else {
          // all the remaining nodes should be reachable from input streams
          // start from input streams again to find the next node that hasn't been visited
          JobNode nextNode = inputStreams.stream().flatMap(input -> input.getTargetNodes().stream())
              .filter(node -> !visited.contains(node))
              .findAny().get();
          q.add(nextNode);
          visited.add(nextNode);
        }
      }
    }

    return sortedNodes;
  }
}
