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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutionPlanner {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  private final Config config;

  public ExecutionPlanner(Config config) {
    this.config = config;
  }

  public ProcessorGraph plan(StreamGraph streamGraph) throws Exception {
    Map<String, SystemAdmin> sysAdmins = getSystemAdmins(config);

    // create physical processors based on stream graph
    ProcessorGraph processorGraph = splitStages(streamGraph);

    if(!processorGraph.getInternalStreams().isEmpty()) {
      // figure out the partition for internal streams
      Multimap<String, StreamSpec> streams = calculatePartitions(streamGraph, processorGraph, sysAdmins);

      // create the streams
      createStreams(streams, sysAdmins);
    }

    return processorGraph;
  }

  public ProcessorGraph splitStages(StreamGraph streamGraph) throws Exception {
    String pipelineId = String.format("%s-%s", config.get(JobConfig.JOB_NAME()), config.getOrDefault(JobConfig.JOB_ID(), "1"));
    // For this phase, we are going to create a processor with the whole dag
    String processorId = pipelineId; // only one processor, name it the same as pipeline itself

    ProcessorGraph processorGraph = new ProcessorGraph(config);

    // TODO: remote the casting once we have the correct types in StreamGraph
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

  private Multimap<String, StreamSpec> calculatePartitions(StreamGraph streamGraph, ProcessorGraph processorGraph, Map<String, SystemAdmin> sysAdmins) {
    // fetch the external streams partition info
    getExistingStreamPartitions(processorGraph, sysAdmins);

    // use BFS to figure out the join partition count


    // TODO this algorithm assumes only one processor, and it does not consider join
    Multimap<String, StreamSpec> streamsGroupedBySystem = HashMultimap.create();
    List<ProcessorNode> processors = processorGraph.topologicalSort();
    processors.forEach(processor -> {
        Set<StreamEdge> outStreams = new HashSet<>(processor.getOutEdges());
        outStreams.retainAll(processorGraph.getInternalStreams());
        if (!outStreams.isEmpty()) {
          int maxInPartition = maxPartition(processor.getInEdges());
          int maxOutPartition = maxPartition(processor.getOutEdges());
          int partition = Math.max(maxInPartition, maxOutPartition);

          outStreams.forEach(streamEdge -> {
              if (streamEdge.getPartitions() == -1) {
                streamEdge.setPartitions(partition);
                StreamSpec streamSpec = createStreamSpec(streamEdge);
                streamsGroupedBySystem.put(streamEdge.getSystemStream().getSystem(), streamSpec);
              }
            });
        }
      });

    return streamsGroupedBySystem;
  }

  private void getExistingStreamPartitions(ProcessorGraph processorGraph, Map<String, SystemAdmin> sysAdmins) {
    Set<StreamEdge> allStreams = new HashSet<>();
    allStreams.addAll(processorGraph.getSources());
    allStreams.addAll(processorGraph.getSinks());
    allStreams.addAll(processorGraph.getInternalStreams());

    Multimap<String, StreamEdge> externalStreamsMap = HashMultimap.create();
    allStreams.forEach(streamEdge -> {
      SystemStream systemStream = streamEdge.getSystemStream();
      externalStreamsMap.put(systemStream.getSystem(), streamEdge);
    });
    for (Map.Entry<String, Collection<StreamEdge>> entry : externalStreamsMap.asMap().entrySet()) {
      String systemName = entry.getKey();
      Collection<StreamEdge> streamEdges = entry.getValue();
      Map<String, StreamEdge> streamToEdge = new HashMap<>();
      streamEdges.forEach(streamEdge -> streamToEdge.put(streamEdge.getSystemStream().getStream(), streamEdge));
      SystemAdmin systemAdmin = sysAdmins.get(systemName);
      Map<String, SystemStreamMetadata> metadata = systemAdmin.getSystemStreamMetadata(streamToEdge.keySet());
      metadata.forEach((stream, data) -> {
          int partitions = data.getSystemStreamPartitionMetadata().size();
          streamToEdge.get(stream).setPartitions(partitions);
          log.info("Partition count is {} for stream {}", partitions, stream);
        });
    }
  }

  private void createStreams(Multimap<String, StreamSpec> streams, Map<String, SystemAdmin> sysAdmins) {
    for (Map.Entry<String, Collection<StreamSpec>> entry : streams.asMap().entrySet()) {
      String systemName = entry.getKey();
      SystemAdmin systemAdmin = sysAdmins.get(systemName);

      for (StreamSpec stream : entry.getValue()) {
        log.info("Creating stream {} on system {}", stream.getPhysicalName(), systemName);
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
