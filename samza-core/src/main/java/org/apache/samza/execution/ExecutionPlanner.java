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
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.execution.ExecutionPlanner.JoinedStreamsGroup.JoinedStreamsGroupCategory;
import static org.apache.samza.util.StreamUtil.*;


/**
 * The ExecutionPlanner creates the physical execution graph for the {@link OperatorSpecGraph}, and
 * the intermediate topics needed for the execution.
 */
// TODO: ExecutionPlanner needs to be able to generate single node JobGraph for low-level TaskApplication as well (SAMZA-1811)
public class ExecutionPlanner {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  /* package private */ static final int MAX_INFERRED_PARTITIONS = 256;

  private final Config config;
  private final StreamConfig streamConfig;
  private final StreamManager streamManager;

  public ExecutionPlanner(Config config, StreamManager streamManager) {
    this.config = config;
    this.streamManager = streamManager;
    this.streamConfig = new StreamConfig(config);
  }

  public ExecutionPlan plan(OperatorSpecGraph specGraph) {
    validateConfig();

    // Create physical job graph based on stream graph
    JobGraph jobGraph = createJobGraph(specGraph);

    // Fetch the external streams partition info
    fetchInputAndOutputStreamPartitions(jobGraph);

    // Verify agreement in partition count between all joined input/intermediate streams
    validateJoinInputStreamPartitions(jobGraph);

    if (!jobGraph.getIntermediateStreamEdges().isEmpty()) {
      // Set partition count of intermediate streams not participating in joins
      setIntermediateStreamPartitions(jobGraph);

      // Validate partition counts were assigned for all intermediate streams
      validateIntermediateStreamPartitions(jobGraph);
    }

    return jobGraph;
  }

  private void validateConfig() {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    ClusterManagerConfig clusterConfig = new ClusterManagerConfig(config);
    // currently we don't support host-affinity in batch mode
    if (appConfig.getAppMode() == ApplicationConfig.ApplicationMode.BATCH && clusterConfig.getHostAffinityEnabled()) {
      throw new SamzaException(String.format("Host affinity is not supported in batch mode. Please configure %s=false.",
          ClusterManagerConfig.CLUSTER_MANAGER_HOST_AFFINITY_ENABLED));
    }
  }

  /**
   * Creates the physical graph from {@link OperatorSpecGraph}
   */
  /* package private */ JobGraph createJobGraph(OperatorSpecGraph specGraph) {
    JobGraph jobGraph = new JobGraph(config, specGraph);

    // Source streams contain both input and intermediate streams.
    Set<StreamSpec> sourceStreams = getStreamSpecs(specGraph.getInputOperators().keySet(), streamConfig);
    // Sink streams contain both output and intermediate streams.
    Set<StreamSpec> sinkStreams = getStreamSpecs(specGraph.getOutputStreams().keySet(), streamConfig);

    Set<StreamSpec> intermediateStreams = Sets.intersection(sourceStreams, sinkStreams);
    Set<StreamSpec> inputStreams = Sets.difference(sourceStreams, intermediateStreams);
    Set<StreamSpec> outputStreams = Sets.difference(sinkStreams, intermediateStreams);

    Set<TableSpec> tables = specGraph.getTables().keySet();

    // For this phase, we have a single job node for the whole dag
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = config.get(JobConfig.JOB_ID(), "1");
    JobNode node = jobGraph.getOrCreateJobNode(jobName, jobId);

    // Add input streams
    inputStreams.forEach(spec -> jobGraph.addInputStream(spec, node));

    // Add output streams
    outputStreams.forEach(spec -> jobGraph.addOutputStream(spec, node));

    // Add intermediate streams
    intermediateStreams.forEach(spec -> jobGraph.addIntermediateStream(spec, node, node));

    // Add tables
    tables.forEach(spec -> jobGraph.addTable(spec, node));

    jobGraph.validate();

    return jobGraph;
  }

  /**
   * Fetches the partitions of input/output streams and update the corresponding StreamEdges.
   */
  /* package private */ void fetchInputAndOutputStreamPartitions(JobGraph jobGraph) {
    Set<StreamEdge> existingStreams = new HashSet<>();
    existingStreams.addAll(jobGraph.getInputStreams());
    existingStreams.addAll(jobGraph.getOutputStreams());

    // System to StreamEdges
    Multimap<String, StreamEdge> systemToStreamEdges = HashMultimap.create();

    // Group StreamEdges by system
    for (StreamEdge streamEdge : existingStreams) {
      String system = streamEdge.getSystemStream().getSystem();
      systemToStreamEdges.put(system, streamEdge);
    }

    // Fetch partition count for every set of StreamEdges belonging to a particular system.
    for (String system : systemToStreamEdges.keySet()) {
      Collection<StreamEdge> streamEdges = systemToStreamEdges.get(system);

      // Map every stream to its corresponding StreamEdge so we can retrieve a StreamEdge given its stream.
      Map<String, StreamEdge> streamToStreamEdge = new HashMap<>();
      for (StreamEdge streamEdge : streamEdges) {
        streamToStreamEdge.put(streamEdge.getSystemStream().getStream(), streamEdge);
      }

      // Retrieve partition count for every set of streams.
      Set<String> streams = streamToStreamEdge.keySet();
      Map<String, Integer> streamToPartitionCount = streamManager.getStreamPartitionCounts(system, streams);

      // Retrieve StreamEdge corresponding to every stream and set partition count on it.
      for (Map.Entry<String, Integer> entry : streamToPartitionCount.entrySet()) {
        String stream = entry.getKey();
        Integer partitionCount = entry.getValue();
        streamToStreamEdge.get(stream).setPartitionCount(partitionCount);
        log.info("Fetched partition count value {} for stream {}", partitionCount, stream);
      }
    }
  }

  /**
   * Validates agreement in partition count between input/intermediate streams participating in join operations.
   */
  private void validateJoinInputStreamPartitions(JobGraph jobGraph) {
    // Group joined input operator specs (input/intermediate streams) by the join
    // operator specs corresponding to the join operations they participate in.
    Multimap<JoinOperatorSpec, InputOperatorSpec> joinOpSpecToInputOpSpecs =
        OperatorSpecGraphAnalyzer.getJoinedInputOperatorSpecs(jobGraph.getSpecGraph());

    // Convert every group of input operator specs into a group of corresponding stream edges.
    List<JoinedStreamsGroup> joinedStreamsGroups = new ArrayList<>();
    for (JoinOperatorSpec joinOpSpec : joinOpSpecToInputOpSpecs.keySet()) {
      Collection<InputOperatorSpec> joinedInputOpSpecs = joinOpSpecToInputOpSpecs.get(joinOpSpec);
      JoinedStreamsGroup joinedStreamsGroup = getJoinedStreamsGroup(joinOpSpec.getOpId(), joinedInputOpSpecs, jobGraph);
      joinedStreamsGroups.add(joinedStreamsGroup);
    }

    /*
     * Sort the stream edge groups by their category so they appear in this order:
     *   1. groups composed exclusively of stream edges with set partition counts
     *   2. groups composed of a mix of stream edges  with set/unset partition counts
     *   3. groups composed exclusively of stream edges with unset partition counts
     *
     *   This guarantees that we process the most constrained stream edge groups first,
     *   which is crucial for intermediate stream edges that are members of multiple
     *   stream edge groups. For instance, if we have the following groups of stream
     *   edges (partition counts in parentheses, question marks for intermediate streams):
     *
     *      a. e1 (16), e2 (16)
     *      b. e2 (16), e3 (?)
     *      c. e3 (?), e4 (?)
     *
     *   processing them in the above order (most constrained first) is guaranteed to
     *   yield correct assignment of partition counts of e3 and e4 in a single scan.
     */
    Collections.sort(joinedStreamsGroups, Comparator.comparing(JoinedStreamsGroup::getCategory));

    // Verify agreement between joined input/intermediate streams.
    // This may involve setting partition counts of intermediate stream edges.
    joinedStreamsGroups.forEach(ExecutionPlanner::validateJoinedStreamsGroupPartitions);
  }

  /**
   * Creates a {@link JoinedStreamsGroup} whose Id is {@code groupId}, and {@link StreamEdge}s
   * correspond to the provided {@code inputOperatorSpecs}.
   */
  private JoinedStreamsGroup getJoinedStreamsGroup(String groupId, Iterable<InputOperatorSpec> inputOperatorSpecs,
      JobGraph jobGraph) {

    int countStreamEdgeWithSetPartitions = 0;
    Set<StreamEdge> streamEdges = new HashSet<>();

    for (InputOperatorSpec inputOpSpec : inputOperatorSpecs) {
      StreamEdge streamEdge = jobGraph.getOrCreateStreamEdge(getStreamSpec(inputOpSpec.getStreamId(), streamConfig));
      if (streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN) {
        ++countStreamEdgeWithSetPartitions;
      }
      streamEdges.add(streamEdge);
    }

    // Determine category of stream group based on stream partition counts.
    JoinedStreamsGroupCategory category;
    if (countStreamEdgeWithSetPartitions == 0) {
      category = JoinedStreamsGroupCategory.NO_PARTITION_COUNT_SET;
    } else if (countStreamEdgeWithSetPartitions == streamEdges.size()) {
      category = JoinedStreamsGroupCategory.ALL_PARTITION_COUNT_SET;
    } else {
      category = JoinedStreamsGroupCategory.SOME_PARTITION_COUNT_SET;
    }

    return new JoinedStreamsGroup(groupId, streamEdges, category);
  }

  /**
   * Sets partition count of intermediate streams which have not been assigned partition counts.
   */
  private void setIntermediateStreamPartitions(JobGraph jobGraph) {
    final String defaultPartitionsConfigProperty = JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS();
    int partitions = config.getInt(defaultPartitionsConfigProperty, StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions == StreamEdge.PARTITIONS_UNKNOWN) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      // partition will be further bounded by MAX_INFERRED_PARTITIONS.
      // This is important when running in hadoop where an HDFS input can have lots of files (partitions).
      int maxInPartitions = maxPartitions(jobGraph.getInputStreams());
      int maxOutPartitions = maxPartitions(jobGraph.getOutputStreams());
      partitions = Math.max(maxInPartitions, maxOutPartitions);

      if (partitions > MAX_INFERRED_PARTITIONS) {
        partitions = MAX_INFERRED_PARTITIONS;
        log.warn(String.format("Inferred intermediate stream partition count %d is greater than the max %d. Using the max.",
            partitions, MAX_INFERRED_PARTITIONS));
      }
    } else {
      // Reject any zero or other negative values explicitly specified in config.
      if (partitions <= 0) {
        throw new SamzaException(String.format("Invalid value %d specified for config property %s", partitions,
            defaultPartitionsConfigProperty));
      }

      log.info("Using partition count value {} specified for config property {}", partitions,
          defaultPartitionsConfigProperty);
    }

    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        log.info("Set the partition count for intermediate stream {} to {}.", edge.getName(), partitions);
        edge.setPartitionCount(partitions);
      }
    }
  }

  /**
   * Ensures all intermediate streams have been assigned partition counts.
   */
  private static void validateIntermediateStreamPartitions(JobGraph jobGraph) {
    for (StreamEdge edge : jobGraph.getIntermediateStreamEdges()) {
      if (edge.getPartitionCount() <= 0) {
        throw new SamzaException(String.format("Failed to assign valid partition count to Stream %s", edge.getName()));
      }
    }
  }

  /**
   * Ensures that all streams in supplied {@link JoinedStreamsGroup} agree in partition count.
   * This includes setting partition counts of any intermediate streams that do not have their
   * partition counts set.
   */
  private static void validateJoinedStreamsGroupPartitions(JoinedStreamsGroup joinedStreamsGroup) {
    Set<StreamEdge> streamEdges = joinedStreamsGroup.getStreamEdges();
    StreamEdge firstStreamEdgeWithSetPartitions =
        streamEdges.stream()
            .filter(streamEdge -> streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN)
            .findFirst()
            .orElse(null);

    /*
     * This group consists exclusively of intermediate streams with unknown partition counts.
     * We cannot do any validation/computation of partition counts of such streams right here,
     * but they are tackled later in the ExecutionPlanner.
     */
    if (firstStreamEdgeWithSetPartitions == null) {
      return;
    }

    // Make sure all other stream edges in this group have the same partition count.
    int partitions = firstStreamEdgeWithSetPartitions.getPartitionCount();
    for (StreamEdge streamEdge : streamEdges) {
      int streamPartitions = streamEdge.getPartitionCount();
      if (streamPartitions == StreamEdge.PARTITIONS_UNKNOWN) {
        streamEdge.setPartitionCount(partitions);
        log.info("Inferred the partition count {} for the join operator {} from {}."
            , new Object[] {partitions, joinedStreamsGroup.getGroupId(), firstStreamEdgeWithSetPartitions.getName()});
      } else if (streamPartitions != partitions) {
        throw  new SamzaException(String.format(
            "Unable to resolve input partitions of stream %s for the join %s. Expected: %d, Actual: %d",
            streamEdge.getName(), joinedStreamsGroup.getGroupId(), partitions, streamPartitions));
      }
    }
  }

  /* package private */ static int maxPartitions(Collection<StreamEdge> edges) {
    return edges.stream().mapToInt(StreamEdge::getPartitionCount).max().orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }

  /**
   * Represents a group of {@link StreamEdge}s participating in a Join operation.
   */
  /* package private */ static class JoinedStreamsGroup {

    /**
     * Indicates whether all stream edges in this group have their partition counts assigned.
     */
    public enum JoinedStreamsGroupCategory {
      /**
       * All stream edges in this group have their partition counts assigned.
       */
      ALL_PARTITION_COUNT_SET,

      /**
       * Only some stream edges in this group have their partition counts assigned.
       */
      SOME_PARTITION_COUNT_SET,

      /**
       * No stream edge in this group is assigned a partition count.
       */
      NO_PARTITION_COUNT_SET
    }

    private final String groupId;
    private final Set<StreamEdge> streamEdges;
    private final JoinedStreamsGroupCategory category;

    public JoinedStreamsGroup(String groupId, Set<StreamEdge> streamEdges, JoinedStreamsGroupCategory category) {
      this.groupId = groupId;
      this.streamEdges = streamEdges;
      this.category = category;
    }

    public Set<StreamEdge> getStreamEdges() {
      return streamEdges;
    }

    public String getGroupId() {
      return groupId;
    }

    public JoinedStreamsGroupCategory getCategory() {
      return category;
    }
  }
}
