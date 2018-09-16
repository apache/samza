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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationDescriptor;
import org.apache.samza.application.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.StreamUtil.*;


/**
 * The ExecutionPlanner creates the physical execution graph for the {@link ApplicationDescriptorImpl}, and
 * the intermediate topics needed for the execution.
 */
// TODO: ExecutionPlanner needs to be able to generate single node JobGraph for low-level TaskApplication as well (SAMZA-1811)
public class ExecutionPlanner {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  private final Config config;
  private final StreamManager streamManager;

  public ExecutionPlanner(Config config, StreamManager streamManager) {
    this.config = config;
    this.streamManager = streamManager;
  }

  public ExecutionPlan plan(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc) {
    validateConfig();

    // create physical job graph based on stream graph
    JobGraph jobGraph = createJobGraph(config, appDesc, new JobGraphJsonGenerator(), new JobNodeConfigureGenerator());

    // fetch the external streams partition info
    updateExistingPartitions(jobGraph, streamManager);

    if (!jobGraph.getIntermediateStreamEdges().isEmpty()) {
      // figure out the partitions for internal streams
      new IntermediateStreamPartitionPlanner(config, appDesc).calculatePartitions(jobGraph);
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
   * Create the physical graph from {@link ApplicationDescriptorImpl}
   */
  /* package private */
  JobGraph createJobGraph(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      JobGraphJsonGenerator jobJsonGenerator, JobNodeConfigureGenerator jobConfigureGenerator) {
    JobGraph jobGraph = new JobGraph(config, appDesc, jobJsonGenerator, jobConfigureGenerator);
    StreamConfig streamConfig = new StreamConfig(config);
    Set<StreamSpec> sourceStreams = getStreamSpecs(appDesc.getInputStreamIds(), streamConfig);
    Set<StreamSpec> sinkStreams = getStreamSpecs(appDesc.getOutputStreamIds(), streamConfig);
    Set<StreamSpec> intStreams = new HashSet<>(sourceStreams);
    Set<TableSpec> tables = appDesc.getTableDescriptors().stream()
        .map(tableDescriptor -> ((BaseTableDescriptor) tableDescriptor).getTableSpec()).collect(Collectors.toSet());
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

    if (!LegacyTaskApplication.class.isAssignableFrom(appDesc.getAppClass())) {
      // skip the validation when input streamIds are empty. This is only possible for LegacyApplication
      jobGraph.validate();
    }

    return jobGraph;
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
}
