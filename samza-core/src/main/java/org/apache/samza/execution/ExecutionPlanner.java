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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.StreamUtil.*;


/**
 * The ExecutionPlanner creates the physical execution graph for the {@link OperatorSpecGraph}, and
 * the intermediate topics needed for the execution.
 */
// TODO: ExecutionPlanner needs to be able to generate single node JobGraph for low-level TaskApplication as well (SAMZA-1811)
public class ExecutionPlanner {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  static final int MAX_INFERRED_PARTITIONS = 256;

  private final Config config;
  private final StreamManager streamManager;

  public ExecutionPlanner(Config config, StreamManager streamManager) {
    this.config = config;
    this.streamManager = streamManager;
  }

  public ExecutionPlan plan(OperatorSpecGraph specGraph) {
    validateConfig();

    // create physical job graph based on stream graph
    JobGraph jobGraph = createJobGraph(specGraph);

    // fetch the external streams partition info
    updateExistingPartitions(jobGraph, streamManager);

    if (!jobGraph.getIntermediateStreamEdges().isEmpty()) {
      // figure out the partitions for internal streams
      calculatePartitions(jobGraph);
    }

    return jobGraph;
  }

  private void validateConfig() {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    ClusterManagerConfig clusterConfig = new ClusterManagerConfig(config);
    // currently we don't support host-affinity in batch mode
    if (appConfig.getAppMode() == ApplicationConfig.ApplicationMode.BATCH
        && clusterConfig.getHostAffinityEnabled()) {
      throw new SamzaException("Host affinity is not supported in batch mode. Please configure job.host-affinity.enabled=false.");
    }
  }

  /**
   * Create the physical graph from {@link OperatorSpecGraph}
   */
  /* package private */ JobGraph createJobGraph(OperatorSpecGraph specGraph) {
    JobGraph jobGraph = new JobGraph(config, specGraph);
    StreamConfig streamConfig = new StreamConfig(config);
    Set<StreamSpec> sourceStreams = getStreamSpecs(specGraph.getInputOperators().keySet(), streamConfig);
    Set<StreamSpec> sinkStreams = getStreamSpecs(specGraph.getOutputStreams().keySet(), streamConfig);
    Set<StreamSpec> intStreams = new HashSet<>(sourceStreams);
    Set<TableSpec> tables = new HashSet<>(specGraph.getTables().keySet());
    intStreams.retainAll(sinkStreams);
    sourceStreams.removeAll(intStreams);
    sinkStreams.removeAll(intStreams);

    // For this phase, we have a single job node for the whole dag
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = config.get(JobConfig.JOB_ID(), "1");
    JobNode node = jobGraph.getOrCreateJobNode(jobName, jobId);

    // add sources
    sourceStreams.forEach(spec -> jobGraph.addSource(spec, node));

    // add sinks
    sinkStreams.forEach(spec -> jobGraph.addSink(spec, node));

    // add intermediate streams
    intStreams.forEach(spec -> jobGraph.addIntermediateStream(spec, node, node));

    // add tables
    tables.forEach(spec -> jobGraph.addTable(spec, node));

    jobGraph.validate();

    return jobGraph;
  }

  /**
   * Figure out the number of partitions of all streams
   */
  /* package private */ void calculatePartitions(JobGraph jobGraph) {
    // calculate the partitions for the input streams of join operators
    calculateJoinInputPartitions(jobGraph, config);

    // calculate the partitions for the rest of intermediate streams
    calculateIntStreamPartitions(jobGraph, config);

    // validate all the partitions are assigned
    validatePartitions(jobGraph);
  }

  /**
   * Fetch the partitions of source/sink streams and update the StreamEdges.
   * @param jobGraph {@link JobGraph}
   * @param streamManager the {@link StreamManager} to interface with the streams.
   */
  /* package private */ static void updateExistingPartitions(JobGraph jobGraph, StreamManager streamManager) {
    Set<StreamEdge> existingStreams = new HashSet<>();
    existingStreams.addAll(jobGraph.getSources());
    existingStreams.addAll(jobGraph.getSinks());

    Multimap<String, StreamEdge> systemToStreamEdges = HashMultimap.create();
    // group the StreamEdge(s) based on the system name
    existingStreams.forEach(streamEdge -> {
        SystemStream systemStream = streamEdge.getSystemStream();
        systemToStreamEdges.put(systemStream.getSystem(), streamEdge);
      });
    for (Map.Entry<String, Collection<StreamEdge>> entry : systemToStreamEdges.asMap().entrySet()) {
      String systemName = entry.getKey();
      Collection<StreamEdge> streamEdges = entry.getValue();
      Map<String, StreamEdge> streamToStreamEdge = new HashMap<>();
      // create the stream name to StreamEdge mapping for this system
      streamEdges.forEach(streamEdge -> streamToStreamEdge.put(streamEdge.getSystemStream().getStream(), streamEdge));
      // retrieve the partition counts for the streams in this system
      Map<String, Integer> streamToPartitionCount = streamManager.getStreamPartitionCounts(systemName, streamToStreamEdge.keySet());
      // set the partitions of a stream to its StreamEdge
      streamToPartitionCount.forEach((stream, partitionCount) -> {
          streamToStreamEdge.get(stream).setPartitionCount(partitionCount);
          log.info("Partition count is {} for stream {}", partitionCount, stream);
        });
    }
  }

  /**
   * Calculate the partitions for the input streams of join operators
   */
  /* package private */ static void calculateJoinInputPartitions(JobGraph jobGraph, Config config) {
    // mapping from a source stream to all join specs reachable from it
    Multimap<OperatorSpec, StreamEdge> joinSpecToStreamEdges = HashMultimap.create();
    // reverse mapping of the above
    Multimap<StreamEdge, OperatorSpec> streamEdgeToJoinSpecs = HashMultimap.create();
    // A queue of joins with known input partitions
    Queue<OperatorSpec> joinQ = new LinkedList<>();
    // The visited set keeps track of the join specs that have been already inserted in the queue before
    Set<OperatorSpec> visited = new HashSet<>();

    StreamConfig streamConfig = new StreamConfig(config);

    jobGraph.getSpecGraph().getInputOperators().forEach((key, value) -> {
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

  private static void calculateIntStreamPartitions(JobGraph jobGraph, Config config) {
    int partitions = config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions < 0) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      // partition will be further bounded by MAX_INFERRED_PARTITIONS.
      // This is important when running in hadoop where an HDFS input can have lots of files (partitions).
      int maxInPartitions = maxPartition(jobGraph.getSources());
      int maxOutPartitions = maxPartition(jobGraph.getSinks());
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
