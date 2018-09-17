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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationDescriptor;
import org.apache.samza.application.ApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.StreamUtil.getStreamSpec;


/**
 * {@link IntermediateStreamPartitionPlanner} calculates intermediate stream partitions based on the high-level application graph.
 */
class IntermediateStreamPartitionPlanner {

  private static final Logger log = LoggerFactory.getLogger(IntermediateStreamPartitionPlanner.class);

  private final Config config;
  private final Map<String, InputOperatorSpec> inputOperators;

  @VisibleForTesting
  static final int MAX_INFERRED_PARTITIONS = 256;

  IntermediateStreamPartitionPlanner(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc) {
    this.config = config;
    this.inputOperators = appDesc.getInputOperators();
  }

  /**
   * Figure out the number of partitions of all streams
   */
  /* package private */ void calculatePartitions(JobGraph jobGraph) {

    // calculate the partitions for the input streams of join operators
    calculateJoinInputPartitions(jobGraph);

    // calculate the partitions for the rest of intermediate streams
    calculateIntStreamPartitions(jobGraph);

    // validate all the partitions are assigned
    validatePartitions(jobGraph);
  }

  /**
   * Calculate the partitions for the input streams of join operators
   */
  /* package private */ void calculateJoinInputPartitions(JobGraph jobGraph) {
    // mapping from a source stream to all join specs reachable from it
    Multimap<OperatorSpec, StreamEdge> joinSpecToStreamEdges = HashMultimap.create();
    // reverse mapping of the above
    Multimap<StreamEdge, OperatorSpec> streamEdgeToJoinSpecs = HashMultimap.create();
    // A queue of joins with known input partitions
    Queue<OperatorSpec> joinQ = new LinkedList<>();
    // The visited set keeps track of the join specs that have been already inserted in the queue before
    Set<OperatorSpec> visited = new HashSet<>();

    StreamConfig streamConfig = new StreamConfig(config);

    inputOperators.forEach((key, value) -> {
        StreamEdge streamEdge = jobGraph.getOrCreateStreamEdge(getStreamSpec(key, streamConfig));
        // Traverses the StreamGraph to find and update mappings for all Joins reachable from this input StreamEdge
        findReachableJoins(value, streamEdge, joinSpecToStreamEdges, streamEdgeToJoinSpecs, joinQ, visited);
      });

    // At this point, joinQ contains joinSpecs where at least one of the input stream edge partitions is known.
    while (!joinQ.isEmpty()) {
      OperatorSpec join = joinQ.poll();
      int partitions = StreamEdge.PARTITIONS_UNKNOWN;
      // loop through the input streams to the join and find the partition count
      for (StreamEdge edge : joinSpecToStreamEdges.get(join)) {
        int edgePartitions = edge.getPartitionCount();
        if (edgePartitions != StreamEdge.PARTITIONS_UNKNOWN) {
          if (partitions == StreamEdge.PARTITIONS_UNKNOWN) {
            //if the partition is not assigned
            partitions = edgePartitions;
            log.info("Inferred the partition count {} for the join operator {} from {}."
                , new Object[] {partitions, join.getOpId(), edge.getName()});
          } else if (partitions != edgePartitions) {
            throw  new SamzaException(String.format(
                "Unable to resolve input partitions of stream %s for the join %s. Expected: %d, Actual: %d",
                edge.getName(), join.getOpId(), partitions, edgePartitions));
          }
        }
      }

      // assign the partition count for intermediate streams
      for (StreamEdge edge : joinSpecToStreamEdges.get(join)) {
        if (edge.getPartitionCount() <= 0) {
          log.info("Set the partition count to {} for input stream {} to the join {}.",
              new Object[] {partitions, edge.getName(), join.getOpId()});
          edge.setPartitionCount(partitions);

          // find other joins can be inferred by setting this edge
          for (OperatorSpec op : streamEdgeToJoinSpecs.get(edge)) {
            if (!visited.contains(op)) {
              joinQ.add(op);
              visited.add(op);
            }
          }
        }
      }
    }
  }

  /**
   * This function traverses the {@link OperatorSpec} graph to find and update mappings for all Joins reachable
   * from this input {@link StreamEdge}.
   * @param operatorSpec the {@link OperatorSpec} to traverse
   * @param sourceStreamEdge source {@link StreamEdge}
   * @param joinSpecToStreamEdges mapping from join spec to its source {@link StreamEdge}s
   * @param streamEdgeToJoinSpecs mapping from source {@link StreamEdge} to the join specs that consumes it
   * @param joinQ queue that contains joinSpecs where at least one of the input stream edge partitions is known.
   */
  private static void findReachableJoins(OperatorSpec operatorSpec, StreamEdge sourceStreamEdge,
      Multimap<OperatorSpec, StreamEdge> joinSpecToStreamEdges,
      Multimap<StreamEdge, OperatorSpec> streamEdgeToJoinSpecs,
      Queue<OperatorSpec> joinQ, Set<OperatorSpec> visited) {
    if (operatorSpec instanceof JoinOperatorSpec) {
      joinSpecToStreamEdges.put(operatorSpec, sourceStreamEdge);
      streamEdgeToJoinSpecs.put(sourceStreamEdge, operatorSpec);

      if (!visited.contains(operatorSpec) && sourceStreamEdge.getPartitionCount() > 0) {
        // put the joins with known input partitions into the queue and mark as visited
        joinQ.add(operatorSpec);
        visited.add(operatorSpec);
      }
    }

    Collection<OperatorSpec> registeredOperatorSpecs = operatorSpec.getRegisteredOperatorSpecs();
    for (OperatorSpec registeredOpSpec : registeredOperatorSpecs) {
      findReachableJoins(registeredOpSpec, sourceStreamEdge, joinSpecToStreamEdges, streamEdgeToJoinSpecs, joinQ,
          visited);
    }
  }

  private void calculateIntStreamPartitions(JobGraph jobGraph) {
    int partitions = config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions < 0) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      // partition will be further bounded by MAX_INFERRED_PARTITIONS.
      // This is important when running in hadoop where an HDFS input can have lots of files (partitions).
      int maxInPartitions = maxPartition(jobGraph.getInputStreams());
      int maxOutPartitions = maxPartition(jobGraph.getOutputStreams());
      partitions = Math.max(maxInPartitions, maxOutPartitions);

      if (partitions > MAX_INFERRED_PARTITIONS) {
        partitions = MAX_INFERRED_PARTITIONS;
        log.warn(String.format("Inferred intermediate stream partition count %d is greater than the max %d. Using the max.",
            partitions, MAX_INFERRED_PARTITIONS));
      }
    }
    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        log.info("Set the partition count for intermediate stream {} to {}.", edge.getName(), partitions);
        edge.setPartitionCount(partitions);
      }
    }
  }

  private static void validatePartitions(JobGraph jobGraph) {
    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        throw new SamzaException(String.format("Failure to assign the partitions to Stream %s", edge.getName()));
      }
    }
  }

  /* package private */ static int maxPartition(Collection<StreamEdge> edges) {
    return edges.stream().map(StreamEdge::getPartitionCount).reduce(Integer::max).orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }
}
