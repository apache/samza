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
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
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

  public ExecutionPlanner(Config config) {
    this.config = config;
  }

  public ProcessorGraph plan(StreamGraph streamGraph) throws Exception {
    Map<String, SystemAdmin> sysAdmins = new JavaSystemConfig(config).getSystemAdmins();
    StreamManager streamManager = new StreamManager(sysAdmins);

    // create physical processors based on stream graph
    ProcessorGraph processorGraph = createProcessorGraph(streamGraph);

    if (!processorGraph.getIntermediateStreams().isEmpty()) {
      // figure out the partitions for internal streams
      calculatePartitions(streamGraph, processorGraph, streamManager);
    }

    return processorGraph;
  }

  /**
   * Create the physical graph from StreamGraph
   */
  /* package private */ ProcessorGraph createProcessorGraph(StreamGraph streamGraph) {
    // For this phase, we are going to create a processor for the whole dag
    String processorId = config.get(JobConfig.JOB_NAME()); // only one processor, use the job name

    ProcessorGraph processorGraph = new ProcessorGraph(config);
    Set<StreamSpec> sourceStreams = new HashSet<>(streamGraph.getInStreams().keySet());
    Set<StreamSpec> sinkStreams = new HashSet<>(streamGraph.getOutStreams().keySet());
    Set<StreamSpec> intStreams = new HashSet<>(sourceStreams);
    intStreams.retainAll(sinkStreams);
    sourceStreams.removeAll(intStreams);
    sinkStreams.removeAll(intStreams);

    // add sources
    sourceStreams.forEach(spec -> processorGraph.addSource(spec, processorId));

    // add sinks
    sinkStreams.forEach(spec -> processorGraph.addSink(spec, processorId));

    // add intermediate streams
    intStreams.forEach(spec -> processorGraph.addIntermediateStream(spec, processorId, processorId));

    processorGraph.validate();

    return processorGraph;
  }

  /**
   * Figure out the number of partitions of all streams
   */
  /* package private */ void calculatePartitions(StreamGraph streamGraph, ProcessorGraph processorGraph, StreamManager streamManager) {
    // fetch the external streams partition info
    updateExistingPartitions(processorGraph, streamManager);

    // calculate the partitions for the input streams of join operators
    calculateJoinInputPartitions(streamGraph, processorGraph);

    // calculate the partitions for the rest of intermediate streams
    calculateIntStreamPartitions(processorGraph, config);

    // validate all the partitions are assigned
    validatePartitions(processorGraph);
  }

  /**
   * Fetch the partitions of source/sink streams and update the StreamEdges.
   * @param processorGraph ProcessorGraph
   * @param streamManager the {@StreamManager} to interface with the streams.
   */
  /* package private */ static void updateExistingPartitions(ProcessorGraph processorGraph, StreamManager streamManager) {
    Set<StreamEdge> existingStreams = new HashSet<>();
    existingStreams.addAll(processorGraph.getSources());
    existingStreams.addAll(processorGraph.getSinks());

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
  /* package private */ static void calculateJoinInputPartitions(StreamGraph streamGraph, ProcessorGraph processorGraph) {
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

    streamGraph.getInStreams().entrySet().forEach(entry -> {
        StreamEdge streamEdge = processorGraph.getOrCreateEdge(entry.getKey());
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

  private static void calculateIntStreamPartitions(ProcessorGraph processorGraph, Config config) {
    int partitions = config.getInt(JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS(), StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions < 0) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      int maxInPartitions = maxPartition(processorGraph.getSources());
      int maxOutPartitions = maxPartition(processorGraph.getSinks());
      partitions = Math.max(maxInPartitions, maxOutPartitions);
    }
    for (StreamEdge edge : processorGraph.getIntermediateStreams()) {
      if (edge.getPartitionCount() <= 0) {
        edge.setPartitionCount(partitions);
      }
    }
  }

  private static void validatePartitions(ProcessorGraph processorGraph) {
    for (StreamEdge edge : processorGraph.getIntermediateStreams()) {
      if (edge.getPartitionCount() <= 0) {
        throw new SamzaException(String.format("Failure to assign the partitions to Stream %s", edge.getFormattedSystemStream()));
      }
    }
  }

  /* package private */ static int maxPartition(Collection<StreamEdge> edges) {
    return edges.stream().map(StreamEdge::getPartitionCount).reduce(Integer::max).orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }

}
