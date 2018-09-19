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
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.ListUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IntermediateStreamManager} calculates intermediate stream partitions based on the high-level application graph.
 */
class IntermediateStreamManager {

  private static final Logger log = LoggerFactory.getLogger(IntermediateStreamManager.class);

  private final Config config;
  private final Collection<InputOperatorSpec> inputOpSpecs;

  @VisibleForTesting
  static final int MAX_INFERRED_PARTITIONS = 256;

  IntermediateStreamManager(Config config, Collection<InputOperatorSpec> inputOpSpecs) {
    this.config = config;
    this.inputOpSpecs = inputOpSpecs;
  }

  /**
   * Figure out the number of partitions of all streams
   */
  /* package private */ void calculatePartitions(JobGraph jobGraph) {

    // Verify agreement in partition count between all joined input/intermediate streams
    validateJoinInputStreamPartitions(jobGraph);

    if (!jobGraph.getIntermediateStreamEdges().isEmpty()) {
      // Set partition count of intermediate streams not participating in joins
      setIntermediateStreamPartitions(jobGraph);

      // Validate partition counts were assigned for all intermediate streams
      validateIntermediateStreamPartitions(jobGraph);
    }
  }

  /**
   * Validates agreement in partition count between input/intermediate streams participating in join operations.
   */
  private void validateJoinInputStreamPartitions(JobGraph jobGraph) {
    // Group input operator specs (input/intermediate streams) by the joins they participate in.
    Multimap<OperatorSpec, InputOperatorSpec> joinOpSpecToInputOpSpecs =
        OperatorSpecGraphAnalyzer.getJoinToInputOperatorSpecs(inputOpSpecs);

    // Convert every group of input operator specs into a group of corresponding stream edges.
    List<StreamEdgeSet> streamEdgeSets = new ArrayList<>();
    for (OperatorSpec joinOpSpec : joinOpSpecToInputOpSpecs.keySet()) {
      Collection<InputOperatorSpec> joinedInputOpSpecs = joinOpSpecToInputOpSpecs.get(joinOpSpec);
      StreamEdgeSet streamEdgeSet = getStreamEdgeSet(joinOpSpec.getOpId(), joinedInputOpSpecs, jobGraph);

      // If current join is a stream-table join, add the stream edges corresponding to side-input
      // streams associated with the joined table (if any).
      if (joinOpSpec instanceof StreamTableJoinOperatorSpec) {
        StreamTableJoinOperatorSpec streamTableJoinOperatorSpec = (StreamTableJoinOperatorSpec) joinOpSpec;
        Iterable<String> sideInputs = ListUtils.emptyIfNull(streamTableJoinOperatorSpec.getTableSpec().getSideInputs());
        for (String sideInput : sideInputs) {
          StreamEdge sideInputStreamEdge = jobGraph.getStreamEdge(sideInput);
          streamEdgeSet.getStreamEdges().add(sideInputStreamEdge);
        }
      }

      streamEdgeSets.add(streamEdgeSet);
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
    Collections.sort(streamEdgeSets, Comparator.comparingInt(e -> e.getCategory().getSortOrder()));

    // Verify agreement between joined input/intermediate streams.
    // This may involve setting partition counts of intermediate stream edges.
    streamEdgeSets.forEach(IntermediateStreamManager::validateAndAssignStreamEdgeSetPartitions);
  }

  /**
   * Creates a {@link StreamEdgeSet} whose Id is {@code setId}, and {@link StreamEdge}s
   * correspond to the provided {@code inputOpSpecs}.
   */
  private StreamEdgeSet getStreamEdgeSet(String setId, Iterable<InputOperatorSpec> inputOpSpecs,
      JobGraph jobGraph) {

    int countStreamEdgeWithSetPartitions = 0;
    Set<StreamEdge> streamEdges = new HashSet<>();

    for (InputOperatorSpec inputOpSpec : inputOpSpecs) {
      StreamEdge streamEdge = jobGraph.getStreamEdge(inputOpSpec.getStreamId());
      if (streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN) {
        ++countStreamEdgeWithSetPartitions;
      }
      streamEdges.add(streamEdge);
    }

    // Determine category of stream group based on stream partition counts.
    StreamEdgeSet.StreamEdgeSetCategory category;
    if (countStreamEdgeWithSetPartitions == 0) {
      category = StreamEdgeSet.StreamEdgeSetCategory.NO_PARTITION_COUNT_SET;
    } else if (countStreamEdgeWithSetPartitions == streamEdges.size()) {
      category = StreamEdgeSet.StreamEdgeSetCategory.ALL_PARTITION_COUNT_SET;
    } else {
      category = StreamEdgeSet.StreamEdgeSetCategory.SOME_PARTITION_COUNT_SET;
    }

    return new StreamEdgeSet(setId, streamEdges, category);
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
   * Ensures that all streams in the supplied {@link StreamEdgeSet} agree in partition count.
   * This may include setting partition counts of intermediate streams in this set that do not
   * have their partition counts set.
   */
  private static void validateAndAssignStreamEdgeSetPartitions(StreamEdgeSet streamEdgeSet) {
    Set<StreamEdge> streamEdges = streamEdgeSet.getStreamEdges();
    StreamEdge firstStreamEdgeWithSetPartitions =
        streamEdges.stream()
            .filter(streamEdge -> streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN)
            .findFirst()
            .orElse(null);

    // This group consists exclusively of intermediate streams with unknown partition counts.
    // We cannot do any validation/computation of partition counts of such streams right here,
    // but they are tackled later in the ExecutionPlanner.
    if (firstStreamEdgeWithSetPartitions == null) {
      return;
    }

    // Make sure all other stream edges in this group have the same partition count.
    int partitions = firstStreamEdgeWithSetPartitions.getPartitionCount();
    for (StreamEdge streamEdge : streamEdges) {
      int streamPartitions = streamEdge.getPartitionCount();
      if (streamPartitions == StreamEdge.PARTITIONS_UNKNOWN) {
        streamEdge.setPartitionCount(partitions);
        log.info("Inferred the partition count {} for the join operator {} from {}.",
            new Object[] {partitions, streamEdgeSet.getSetId(), firstStreamEdgeWithSetPartitions.getName()});
      } else if (streamPartitions != partitions) {
        throw  new SamzaException(String.format(
            "Unable to resolve input partitions of stream %s for the join %s. Expected: %d, Actual: %d",
            streamEdge.getName(), streamEdgeSet.getSetId(), partitions, streamPartitions));
      }
    }
  }

  /* package private */ static int maxPartitions(Collection<StreamEdge> edges) {
    return edges.stream().mapToInt(StreamEdge::getPartitionCount).max().orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }

  /**
   * Represents a set of {@link StreamEdge}s.
   */
  /* package private */ static class StreamEdgeSet {

    /**
     * Indicates whether all stream edges in this group have their partition counts assigned.
     */
    public enum StreamEdgeSetCategory {
      /**
       * All stream edges in this group have their partition counts assigned.
       */
      ALL_PARTITION_COUNT_SET(0),

      /**
       * Only some stream edges in this group have their partition counts assigned.
       */
      SOME_PARTITION_COUNT_SET(1),

      /**
       * No stream edge in this group is assigned a partition count.
       */
      NO_PARTITION_COUNT_SET(2);


      private final int sortOrder;

      StreamEdgeSetCategory(int sortOrder) {
        this.sortOrder = sortOrder;
      }

      public int getSortOrder() {
        return sortOrder;
      }
    }

    private final String setId;
    private final Set<StreamEdge> streamEdges;
    private final StreamEdgeSetCategory category;

    StreamEdgeSet(String setId, Set<StreamEdge> streamEdges, StreamEdgeSetCategory category) {
      this.setId = setId;
      this.streamEdges = streamEdges;
      this.category = category;
    }

    Set<StreamEdge> getStreamEdges() {
      return streamEdges;
    }

    String getSetId() {
      return setId;
    }

    StreamEdgeSetCategory getCategory() {
      return category;
    }
  }
}
