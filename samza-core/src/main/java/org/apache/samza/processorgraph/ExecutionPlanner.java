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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.util.Util;
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
    Map<String, SystemAdmin> sysAdmins = getSystemAdmins(config);

    // create physical processors based on stream graph
    ProcessorGraph processorGraph = createProcessorGraph(streamGraph);

    if (!processorGraph.getInternalStreams().isEmpty()) {
      // figure out the partitions for internal streams
      calculatePartitions(streamGraph, processorGraph, sysAdmins);

      // create the streams
      createStreams(processorGraph, sysAdmins);
    }

    return processorGraph;
  }

  /**
   * Create the physical graph from StreamGraph
   * Package private for testing
   */
  ProcessorGraph createProcessorGraph(StreamGraph streamGraph) {
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
    intStreams.forEach(spec -> processorGraph.addEdge(spec, processorId, processorId));

    processorGraph.validate();

    return processorGraph;
  }

  /**
   * Figure out the number of partitions of intermediate streams
   * Package private for testing
   */
  void calculatePartitions(StreamGraph streamGraph, ProcessorGraph processorGraph, Map<String, SystemAdmin> sysAdmins) {
    // fetch the external streams partition info
    fetchExistingStreamPartitions(processorGraph, sysAdmins);

    // calculate the partitions for the input streams of join operators
    calculateJoinInputPartitions(streamGraph, processorGraph);

    // calculate the partitions for the rest of intermediate streams
    calculateIntStreamPartitions(processorGraph, config);
  }

  static void fetchExistingStreamPartitions(ProcessorGraph processorGraph, Map<String, SystemAdmin> sysAdmins) {
    Set<StreamEdge> existingStreams = new HashSet<>();
    existingStreams.addAll(processorGraph.getSources());
    existingStreams.addAll(processorGraph.getSinks());

    Multimap<String, StreamEdge> existingStreamsMap = HashMultimap.create();
    existingStreams.forEach(streamEdge -> {
        SystemStream systemStream = streamEdge.getSystemStream();
        existingStreamsMap.put(systemStream.getSystem(), streamEdge);
      });
    for (Map.Entry<String, Collection<StreamEdge>> entry : existingStreamsMap.asMap().entrySet()) {
      String systemName = entry.getKey();
      Collection<StreamEdge> streamEdges = entry.getValue();
      Map<String, StreamEdge> streamToEdge = new HashMap<>();
      streamEdges.forEach(streamEdge -> streamToEdge.put(streamEdge.getSystemStream().getStream(), streamEdge));
      SystemAdmin systemAdmin = sysAdmins.get(systemName);
      Map<String, SystemStreamMetadata> metadata = systemAdmin.getSystemStreamMetadata(streamToEdge.keySet());
      metadata.forEach((stream, data) -> {
          int partitions = data.getSystemStreamPartitionMetadata().size();
          streamToEdge.get(stream).setPartitions(partitions);
          log.debug("Partition count is {} for stream {}", partitions, stream);
        });
    }
  }

  /**
   * Calculate the partitions for the input streams of join operators
   * Package private for testing
   */
  static void calculateJoinInputPartitions(StreamGraph streamGraph, ProcessorGraph processorGraph) {
    // get join operators with input streams
    Multimap<OperatorSpec, StreamEdge> joinToStreamMap = HashMultimap.create();
    Multimap<StreamEdge, OperatorSpec> streamToJoinMap = HashMultimap.create();
    Map<MessageStream, OperatorSpec> outputToJoinMap = new HashMap<>();
    Queue<OperatorSpec> joinQ = new LinkedList<>(); // a queue of joins with known input partitions
    Set<OperatorSpec> visited = new HashSet<>();
    streamGraph.getInStreams().entrySet().forEach(entry -> {
        StreamEdge streamEdge = processorGraph.getEdge(entry.getKey());
        getJoins(entry.getValue(), streamEdge, joinToStreamMap, streamToJoinMap, outputToJoinMap, joinQ, visited);
      });
    // calculate join input partition count
    while (!joinQ.isEmpty()) {
      OperatorSpec join = joinQ.poll();
      int partitions = -1;
      // loop through the input streams to the join and find the partition count
      for (StreamEdge edge : joinToStreamMap.get(join)) {
        int edgePartitions = edge.getPartitions();
        if (edgePartitions != -1) {
          if (partitions == -1) {
            //if the partition is not assigned
            partitions = edgePartitions;
          } else if (partitions != edgePartitions) {
            throw  new SamzaException(String.format("Unable to resolve input partitions of stream %s for join",
                edge.getSystemStream().toString()));
          }
        }
      }
      // assign the partition count
      for (StreamEdge edge : joinToStreamMap.get(join)) {
        if (edge.getPartitions() <= 0) {
          edge.setPartitions(partitions);

          // find other joins can be inferred by setting this edge
          for (OperatorSpec op : streamToJoinMap.get(edge)) {
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
   * This function
   * @param messageStream
   * @param streamEdge
   * @param joinToStreamMap
   * @param streamToJoinMap
   * @param outputToJoinMap
   * @param joinQ
   * @param visited
   */
  static void getJoins(MessageStream messageStream,
      StreamEdge streamEdge,
      Multimap<OperatorSpec, StreamEdge> joinToStreamMap,
      Multimap<StreamEdge, OperatorSpec> streamToJoinMap,
      Map<MessageStream, OperatorSpec> outputToJoinMap,
      Queue<OperatorSpec> joinQ,
      Set<OperatorSpec> visited) {
    Collection<OperatorSpec> specs = ((MessageStreamImpl) messageStream).getRegisteredOperatorSpecs();
    for (OperatorSpec spec : specs) {
      if (spec instanceof PartialJoinOperatorSpec) {
        // every join will have two partial join operators
        // we will choose one of them in order to consolidate the inputs
        // the first one who registered with the outputToJoinMap will win
        MessageStream output = spec.getNextStream();
        OperatorSpec joinSpec = outputToJoinMap.get(output);
        if (joinSpec == null) {
          joinSpec = spec;
          outputToJoinMap.put(output, joinSpec);
        }

        joinToStreamMap.put(joinSpec, streamEdge);
        streamToJoinMap.put(streamEdge, joinSpec);

        if (!visited.contains(joinSpec) && streamEdge.getPartitions() > 0) {
          // put the joins with known input partitions into the queue
          joinQ.add(joinSpec);
          visited.add(joinSpec);
        }
      }

      if (spec.getNextStream() != null) {
        getJoins(spec.getNextStream(), streamEdge, joinToStreamMap, streamToJoinMap, outputToJoinMap, joinQ, visited);
      }
    }
  }

  static void calculateIntStreamPartitions(ProcessorGraph processorGraph, Config config) {
    int partitions = config.getInt(JobConfig.JOB_DEFAULT_PARTITIONS(), -1);
    if (partitions < 0) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      int maxInPartitions = maxPartition(processorGraph.getSources());
      int maxOutPartitions = maxPartition(processorGraph.getSinks());
      partitions = Math.max(maxInPartitions, maxOutPartitions);
    }
    for (StreamEdge edge : processorGraph.getInternalStreams()) {
      if (edge.getPartitions() <= 0) {
        edge.setPartitions(partitions);
      }
    }
  }

  private static void createStreams(ProcessorGraph graph, Map<String, SystemAdmin> sysAdmins) {
    Multimap<String, StreamSpec> streamsToCreate = HashMultimap.create();
    graph.getInternalStreams().forEach(edge -> {
        StreamSpec streamSpec = createStreamSpec(edge);
        streamsToCreate.put(edge.getSystemStream().getSystem(), streamSpec);
      });

    for (Map.Entry<String, Collection<StreamSpec>> entry : streamsToCreate.asMap().entrySet()) {
      String systemName = entry.getKey();
      SystemAdmin systemAdmin = sysAdmins.get(systemName);

      for (StreamSpec stream : entry.getValue()) {
        log.info("Creating stream {} with partitions {} on system {}",
            new Object[]{stream.getPhysicalName(), stream.getPartitionCount(), systemName});
        systemAdmin.createStream(stream);
      }
    }
  }

  private static int maxPartition(Collection<StreamEdge> edges) {
    return edges.stream().map(StreamEdge::getPartitions).reduce(Integer::max).get();
  }

  private static StreamSpec createStreamSpec(StreamEdge edge) {
    StreamSpec orgSpec = edge.getStreamSpec();
    return orgSpec.copyWithPartitionCount(edge.getPartitions());
  }

  private static Map<String, SystemAdmin> getSystemAdmins(Config config) {
    return getSystemFactories(config).entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getAdmin(entry.getKey(), config)));
  }

  private static Map<String, SystemFactory> getSystemFactories(Config config) {
    Map<String, SystemFactory> systemFactories =
        getSystemNames(config).stream().collect(Collectors.toMap(systemName -> systemName, systemName -> {
            String systemFactoryClassName = new JavaSystemConfig(config).getSystemFactory(systemName);
            if (systemFactoryClassName == null) {
              throw new SamzaException(
                  String.format("A stream uses system %s, which is missing from the configuration.", systemName));
            }
            return Util.getObj(systemFactoryClassName);
          }));

    return systemFactories;
  }

  private static Collection<String> getSystemNames(Config config) {
    return new JavaSystemConfig(config).getSystemNames();
  }
}
