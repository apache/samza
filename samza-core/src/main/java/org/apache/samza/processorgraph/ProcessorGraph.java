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

package org.apache.samza.processorgraph;

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
import org.apache.samza.config.Config;
import org.apache.samza.system.StreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ProcessorGraph is the physical execution graph for a multi-stage Samza application.
 * It contains the topology of execution processors connected with source/sink/intermediate streams.
 * High level APIs are transformed into ProcessorGraph for planing, validation and execution.
 */
public class ProcessorGraph {
  private static final Logger log = LoggerFactory.getLogger(ProcessorGraph.class);

  private final Map<String, ProcessorNode> nodes = new HashMap<>();
  private final Map<String, StreamEdge> edges = new HashMap<>();
  private final Set<StreamEdge> sources = new HashSet<>();
  private final Set<StreamEdge> sinks = new HashSet<>();
  private final Set<StreamEdge> internalStreams = new HashSet<>();
  private final Config config;

  ProcessorGraph(Config config) {
    this.config = config;
  }

  void addSource(StreamSpec input, String targetProcessorId) {
    ProcessorNode node = getNode(targetProcessorId);
    StreamEdge edge = getEdge(input);
    edge.addTargetNode(node);
    node.addInEdge(edge);
    sources.add(edge);
  }

  void addSink(StreamSpec output, String sourceProcessorId) {
    ProcessorNode node = getNode(sourceProcessorId);
    StreamEdge edge = getEdge(output);
    edge.addSourceNode(node);
    node.addOutEdge(edge);
    sinks.add(edge);
  }

  void addEdge(StreamSpec streamSpec, String sourceProcessorId, String targetProcessorId) {
    ProcessorNode sourceNode = getNode(sourceProcessorId);
    ProcessorNode targetNode = getNode(targetProcessorId);
    StreamEdge edge = getEdge(streamSpec);
    edge.addSourceNode(sourceNode);
    edge.addTargetNode(targetNode);
    sourceNode.addOutEdge(edge);
    targetNode.addInEdge(edge);
    internalStreams.add(edge);
  }

  ProcessorNode getNode(String processorId) {
    ProcessorNode node = nodes.get(processorId);
    if (node == null) {
      node = new ProcessorNode(processorId, config);
      nodes.put(processorId, node);
    }
    return node;
  }

  StreamEdge getEdge(StreamSpec streamSpec) {
    String streamId = streamSpec.getId();
    StreamEdge edge = edges.get(streamId);
    if (edge == null) {
      edge = new StreamEdge(streamSpec, config);
      edges.put(streamId, edge);
    }
    return edge;
  }

  /**
   * Returns the processor with configs to be executed in the topological order
   * @return list of ProcessorConfig
   */
  public List<ProcessorNode> getProcessors() {
    List<ProcessorNode> sortedNodes = topologicalSort();
    return Collections.unmodifiableList(sortedNodes);
  }

  public Set<StreamEdge> getSources() {
    return Collections.unmodifiableSet(sources);
  }

  public Set<StreamEdge> getSinks() {
    return Collections.unmodifiableSet(sinks);
  }

  public Set<StreamEdge> getInternalStreams() {
    return Collections.unmodifiableSet(internalStreams);
  }


  /**
   * Validate the graph
   */
  public void validate() {
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
    final Set<ProcessorNode> reachable = findReachable();
    if (reachable.size() != nodes.size()) {
      Set<ProcessorNode> unreachable = new HashSet<>(nodes.values());
      unreachable.removeAll(reachable);
      throw new IllegalArgumentException(String.format("Processors %s cannot be reached from Sources.",
          String.join(", ", unreachable.stream().map(ProcessorNode::getId).collect(Collectors.toList()))));
    }
  }

  /**
   * Find the reachable set of nodes using BFS.
   * Package private for test.
   * @return reachable set of nodes
   */
  Set<ProcessorNode> findReachable() {
    Queue<ProcessorNode> queue = new ArrayDeque<>();
    Set<ProcessorNode> visited = new HashSet<>();

    sources.forEach(source -> {
        List<ProcessorNode> next = source.getTargetNodes();
        queue.addAll(next);
        visited.addAll(next);
      });

    while (!queue.isEmpty()) {
      ProcessorNode node = queue.poll();
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
   * Package private for test.
   * @return topologically sorted ProcessorNode(s)
   */
  List<ProcessorNode> topologicalSort() {
    Collection<ProcessorNode> pnodes = nodes.values();
    Queue<ProcessorNode> q = new ArrayDeque<>();
    Map<String, Long> indegree = new HashMap<>();
    Set<ProcessorNode> visited = new HashSet<>();
    pnodes.forEach(node -> {
        String nid = node.getId();
        //only count the degrees of intermediate streams since sources have degree 0
        long degree = node.getInEdges().stream().filter(e -> !sources.contains(e)).count();
        indegree.put(nid, degree);

        if (degree == 0L) {
          // start from the nodes that only consume from sources
          q.add(node);
          visited.add(node);
        }
      });

    List<ProcessorNode> sortedNodes = new ArrayList<>();
    Set<ProcessorNode> reachable = new HashSet<>();
    while (sortedNodes.size() < pnodes.size()) {
      while (!q.isEmpty()) {
        ProcessorNode node = q.poll();
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
        // The remaining nodes have circles
        // use the following approach to break the circles
        // start from the nodes that are reachable from previous traverse
        reachable.removeAll(sortedNodes);
        if (!reachable.isEmpty()) {
          //find out the nodes with minimal input edge
          long min = Long.MAX_VALUE;
          ProcessorNode minNode = null;
          for (ProcessorNode node : reachable) {
            Long degree = indegree.get(node.getId());
            if (degree < min) {
              min = degree;
              minNode = node;
            }
          }
          // start from the node with minimal input edge again
          q.add(minNode);
        } else {
          // all the remaining nodes should be reachable from sources
          // start from sources again to find the next node that hasn't been visited
          ProcessorNode nextNode = sources.stream().flatMap(source -> source.getTargetNodes().stream())
              .filter(node -> !visited.contains(node))
              .findAny().get();
          q.add(nextNode);
        }
      }
    }

    return sortedNodes;
  }
}
