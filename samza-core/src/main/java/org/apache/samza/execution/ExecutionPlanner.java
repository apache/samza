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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ExecutionPlanner creates the physical execution graph for the StreamGraph, and
 * the intermediate topics needed for the execution.
 */
public class ExecutionPlanner {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  private final Config config;
  private final StreamManager streamManager;

  public ExecutionPlanner(Config config, StreamManager streamManager) {
    this.config = config;
    this.streamManager = streamManager;
  }

  public ExecutionPlan plan(StreamGraphImpl streamGraph) throws Exception {
    // create physical job graph based on stream graph
    JobGraph jobGraph = createJobGraph(streamGraph);

    // fetch the external streams partition info
    updateExistingPartitions(jobGraph, streamManager);

    if (!jobGraph.getIntermediateStreamEdges().isEmpty()) {
      // figure out the partitions for internal streams
      calculatePartitions(streamGraph, jobGraph);
    }

    return jobGraph;
  }

  /**
   * Create the physical graph from StreamGraph
   */
  /* package private */ JobGraph createJobGraph(StreamGraphImpl streamGraph) {
    JobGraph jobGraph = new JobGraph(config);
    Set<StreamSpec> sourceStreams = new HashSet<>(streamGraph.getInputStreams().keySet());
    Set<StreamSpec> sinkStreams = new HashSet<>(streamGraph.getOutputStreams().keySet());
    Set<StreamSpec> intStreams = new HashSet<>(sourceStreams);
    intStreams.retainAll(sinkStreams);
    sourceStreams.removeAll(intStreams);
    sinkStreams.removeAll(intStreams);

    // For this phase, we have a single job node for the whole dag
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = config.get(JobConfig.JOB_ID(), "1");
    JobNode node = jobGraph.getOrCreateNode(jobName, jobId, streamGraph);

    // add sources
    sourceStreams.forEach(spec -> jobGraph.addSource(spec, node));

    // add sinks
    sinkStreams.forEach(spec -> jobGraph.addSink(spec, node));

    // add intermediate streams
    intStreams.forEach(spec -> jobGraph.addIntermediateStream(spec, node, node));

    jobGraph.validate();

    return jobGraph;
  }

  /**
   * Figure out the number of partitions of all streams
   */
  /* package private */ void calculatePartitions(StreamGraphImpl streamGraph, JobGraph jobGraph) {
    // calculate the partitions for the input streams of join operators
    calculateJoinInputPartitions(streamGraph, jobGraph);

    // calculate the partitions for the rest of intermediate streams
    calculateIntStreamPartitions(jobGraph, config);

    // validate all the partitions are assigned
    validatePartitions(jobGraph);
  }

  /**
   * Fetch the partitions of source/sink streams and update the StreamEdges.
   * @param jobGraph {@link JobGraph}
   * @param streamManager the {@StreamManager} to interface with the streams.
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
          log.debug("Partition count is {} for stream {}", partitionCount, stream);
        });
    }
  }

  /**
   * Calculate the partitions for the input streams of join operators
   */
  /* package private */ static void calculateJoinInputPartitions(StreamGraphImpl streamGraph, JobGraph jobGraph) {
    // mapping from a source stream to all join specs reachable from it
    Multimap<OperatorSpec, StreamEdge> joinSpecToStreamEdges = HashMultimap.create();
    // reverse mapping of the above
    Multimap<StreamEdge, OperatorSpec> streamEdgeToJoinSpecs = HashMultimap.create();
    // Mapping from the output stream to the join spec. Since StreamGraph creates two partial join operators for a join and they
    // will have the same output stream, this mapping is used to choose one of them as the unique join spec representing this join
    // (who register first in the map wins).
    Map<MessageStream, OperatorSpec> outputStreamToJoinSpec = new HashMap<>();
    // A queue of joins with known input partitions
    Queue<OperatorSpec> joinQ = new LinkedList<>();
    // The visited set keeps track of the join specs that have been already inserted in the queue before
    Set<OperatorSpec> visited = new HashSet<>();

    streamGraph.getInputStreams().entrySet().forEach(entry -> {
        StreamEdge streamEdge = jobGraph.getOrCreateEdge(entry.getKey());
        // Traverses the StreamGraph to find and update mappings for all Joins reachable from this input StreamEdge
        findReachableJoins(entry.getValue(), streamEdge, joinSpecToStreamEdges, streamEdgeToJoinSpecs,
            outputStreamToJoinSpec, joinQ, visited);
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
          } else if (partitions != edgePartitions) {
            throw  new SamzaException(String.format(
                "Unable to resolve input partitions of stream %s for join. Expected: %d, Actual: %d",
                edge.getFormattedSystemStream(), partitions, edgePartitions));
          }
        }
      }
      // assign the partition count for intermediate streams
      for (StreamEdge edge : joinSpecToStreamEdges.get(join)) {
        if (edge.getPartitionCount() <= 0) {
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
   * This function traverses the StreamGraph to find and update mappings for all Joins reachable from this input StreamEdge
   * @param inputMessageStream next input MessageStream to traverse {@link MessageStream}
   * @param sourceStreamEdge source {@link StreamEdge}
   * @param joinSpecToStreamEdges mapping from join spec to its source {@link StreamEdge}s
   * @param streamEdgeToJoinSpecs mapping from source {@link StreamEdge} to the join specs that consumes it
   * @param outputStreamToJoinSpec mapping from the output stream to the join spec
   * @param joinQ queue that contains joinSpecs where at least one of the input stream edge partitions is known.
   */
  private static void findReachableJoins(MessageStream inputMessageStream, StreamEdge sourceStreamEdge,
      Multimap<OperatorSpec, StreamEdge> joinSpecToStreamEdges, Multimap<StreamEdge, OperatorSpec> streamEdgeToJoinSpecs,
      Map<MessageStream, OperatorSpec> outputStreamToJoinSpec, Queue<OperatorSpec> joinQ, Set<OperatorSpec> visited) {
    Collection<OperatorSpec> specs = ((MessageStreamImpl) inputMessageStream).getRegisteredOperatorSpecs();
    for (OperatorSpec spec : specs) {
      if (spec instanceof PartialJoinOperatorSpec) {
        // every join will have two partial join operators
        // we will choose one of them in order to consolidate the inputs
        // the first one who registered with the outputStreamToJoinSpec will win
        MessageStream output = spec.getNextStream();
        OperatorSpec joinSpec = outputStreamToJoinSpec.get(output);
        if (joinSpec == null) {
          joinSpec = spec;
          outputStreamToJoinSpec.put(output, joinSpec);
        }

        joinSpecToStreamEdges.put(joinSpec, sourceStreamEdge);
        streamEdgeToJoinSpecs.put(sourceStreamEdge, joinSpec);

        if (!visited.contains(joinSpec) && sourceStreamEdge.getPartitionCount() > 0) {
          // put the joins with known input partitions into the queue
          joinQ.add(joinSpec);
          visited.add(joinSpec);
        }
      }

      if (spec.getNextStream() != null) {
        findReachableJoins(spec.getNextStream(), sourceStreamEdge, joinSpecToStreamEdges, streamEdgeToJoinSpecs, outputStreamToJoinSpec, joinQ,
            visited);
      }
    }
  }

  private static void calculateIntStreamPartitions(JobGraph jobGraph, Config config) {
    int partitions = config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions < 0) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      int maxInPartitions = maxPartition(jobGraph.getSources());
      int maxOutPartitions = maxPartition(jobGraph.getSinks());
      partitions = Math.max(maxInPartitions, maxOutPartitions);
    }
    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        edge.setPartitionCount(partitions);
      }
    }
  }

  private static void validatePartitions(JobGraph jobGraph) {
    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        throw new SamzaException(String.format("Failure to assign the partitions to Stream %s", edge.getFormattedSystemStream()));
      }
    }
  }

  /* package private */ static int maxPartition(Collection<StreamEdge> edges) {
    return edges.stream().map(StreamEdge::getPartitionCount).reduce(Integer::max).orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }

}
