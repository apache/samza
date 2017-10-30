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
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.system.StreamSpec;
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
  private final Set<StreamEdge> sources = new HashSet<>();
  private final Set<StreamEdge> sinks = new HashSet<>();
  private final Set<StreamEdge> intermediateStreams = new HashSet<>();
  private final Config config;
  private final JobGraphJsonGenerator jsonGenerator = new JobGraphJsonGenerator();

  /**
   * The JobGraph is only constructed by the {@link ExecutionPlanner}.
   * @param config Config
   */
  JobGraph(Config config) {
    this.config = config;
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
        .map(streamEdge -> streamEdge.getStreamSpec())
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
  public ApplicationConfig getApplicationConfig() {
    return new ApplicationConfig(config);
  }

  /**
   * Add a source stream to a {@link JobNode}
   * @param input source stream
   * @param node the job node that consumes from the source
   */
  void addSource(StreamSpec input, JobNode node) {
    StreamEdge edge = getOrCreateStreamEdge(input);
    edge.addTargetNode(node);
    node.addInEdge(edge);
    sources.add(edge);
  }

  /**
   * Add a sink stream to a {@link JobNode}
   * @param output sink stream
   * @param node the job node that outputs to the sink
   */
  void addSink(StreamSpec output, JobNode node) {
    StreamEdge edge = getOrCreateStreamEdge(output);
    edge.addSourceNode(node);
    node.addOutEdge(edge);
    sinks.add(edge);
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

  /**
   * Get the {@link JobNode}. Create one if it does not exist.
   * @param jobName name of the job
   * @param jobId id of the job
   * @return
   */
  JobNode getOrCreateJobNode(String jobName, String jobId, StreamGraphImpl streamGraph) {
    String nodeId = JobNode.createId(jobName, jobId);
    JobNode node = nodes.get(nodeId);
    if (node == null) {
      node = new JobNode(jobName, jobId, streamGraph, config);
      nodes.put(nodeId, node);
    }
    return node;
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
   * Get the {@link StreamEdge} for a {@link StreamSpec}. Create one if it does not exist.
   * @param streamSpec  spec of the StreamEdge
   * @param isIntermediate  boolean flag indicating whether it's an intermediate stream
   * @return stream edge
   */
  StreamEdge getOrCreateStreamEdge(StreamSpec streamSpec, boolean isIntermediate) {
    String streamId = streamSpec.getId();
    StreamEdge edge = edges.get(streamId);
    if (edge == null) {
      edge = new StreamEdge(streamSpec, isIntermediate, config);
      edges.put(streamId, edge);
    }
    return edge;
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
   * Returns the source streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getSources() {
    return Collections.unmodifiableSet(sources);
  }

  /**
   * Return the sink streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getSinks() {
    return Collections.unmodifiableSet(sinks);
  }

  /**
   * Return the intermediate streams in the graph
   * @return unmodifiable set of {@link StreamEdge}
   */
  Set<StreamEdge> getIntermediateStreamEdges() {
    return Collections.unmodifiableSet(intermediateStreams);
  }

  /**
   * Validate the graph has the correct topology, meaning the sources are coming from external streams,
   * sinks are going to external streams, and the nodes are connected with intermediate streams.
   * Also validate all the nodes are reachable from the sources.
   */
  void validate() {
    validateSources();
    validateSinks();
    validateInternalStreams();
    validateReachability();
  }

  /**
   * Validate the sources should have indegree being 0 and outdegree greater than 0
   */
  private void validateSources() {
    sources.forEach(edge -> {
        if (!edge.getSourceNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Source stream %s should not have producers.", edge.getFormattedSystemStream()));
        }
        if (edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Source stream %s should have consumers.", edge.getFormattedSystemStream()));
        }
      });
  }

  /**
   * Validate the sinks should have outdegree being 0 and indegree greater than 0
   */
  private void validateSinks() {
    sinks.forEach(edge -> {
        if (!edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Sink stream %s should not have consumers", edge.getFormattedSystemStream()));
        }
        if (edge.getSourceNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Sink stream %s should have producers", edge.getFormattedSystemStream()));
        }
      });
  }

  /**
   * Validate the internal streams should have both indegree and outdegree greater than 0
   */
  private void validateInternalStreams() {
    Set<StreamEdge> internalEdges = new HashSet<>(edges.values());
    internalEdges.removeAll(sources);
    internalEdges.removeAll(sinks);

    internalEdges.forEach(edge -> {
        if (edge.getSourceNodes().isEmpty() || edge.getTargetNodes().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Internal stream %s should have both producers and consumers", edge.getFormattedSystemStream()));
        }
      });
  }

  /**
   * Validate all nodes are reachable by sources.
   */
  private void validateReachability() {
    // validate all nodes are reachable from the sources
    final Set<JobNode> reachable = findReachable();
    if (reachable.size() != nodes.size()) {
      Set<JobNode> unreachable = new HashSet<>(nodes.values());
      unreachable.removeAll(reachable);
      throw new IllegalArgumentException(String.format("Jobs %s cannot be reached from Sources.",
          String.join(", ", unreachable.stream().map(JobNode::getId).collect(Collectors.toList()))));
    }
  }

  /**
   * Find the reachable set of nodes using BFS.
   * @return reachable set of {@link JobNode}
   */
  Set<JobNode> findReachable() {
    Queue<JobNode> queue = new ArrayDeque<>();
    Set<JobNode> visited = new HashSet<>();

    sources.forEach(source -> {
        List<JobNode> next = source.getTargetNodes();
        queue.addAll(next);
        visited.addAll(next);
      });

    while (!queue.isEmpty()) {
      JobNode node = queue.poll();
      node.getOutEdges().stream().flatMap(edge -> edge.getTargetNodes().stream()).forEach(target -> {
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
        String nid = node.getId();
        //only count the degrees of intermediate streams
        long degree = node.getInEdges().stream().filter(e -> !sources.contains(e)).count();
        indegree.put(nid, degree);

        if (degree == 0L) {
          // start from the nodes that has no intermediate input streams, so it only consumes from sources
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
        node.getOutEdges().stream().flatMap(edge -> edge.getTargetNodes().stream()).forEach(n -> {
            String nid = n.getId();
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
            Long degree = indegree.get(node.getId());
            if (degree < min) {
              min = degree;
              minNode = node;
            }
          }
          // start from the node with minimal input edge again
          q.add(minNode);
          visited.add(minNode);
        } else {
          // all the remaining nodes should be reachable from sources
          // start from sources again to find the next node that hasn't been visited
          JobNode nextNode = sources.stream().flatMap(source -> source.getTargetNodes().stream())
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
