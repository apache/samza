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
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.execution.ExecutionPlanner.StreamSet;
import static org.apache.samza.execution.ExecutionPlanner.StreamSet.StreamSetCategory;


/**
 * {@link IntermediateStreamManager} calculates intermediate stream partitions based on the high-level application graph.
 */
class IntermediateStreamManager {

  private static final Logger log = LoggerFactory.getLogger(IntermediateStreamManager.class);

  private final Config config;

  @VisibleForTesting
  static final int MAX_INFERRED_PARTITIONS = 256;

  IntermediateStreamManager(Config config) {
    this.config = config;
  }

  /**
   * Calculate the number of partitions of all intermediate streams
   */
  /* package private */ void calculatePartitions(JobGraph jobGraph, Collection<StreamSet> joinedStreamSets) {

    // First, sort the stream edge groups by their category so they appear in the following order:
    //   1. groups composed exclusively of stream edges with set partition counts
    //   2. groups composed of a mix of stream edges  with set/unset partition counts
    //   3. groups composed exclusively of stream edges with unset partition counts
    //
    // This guarantees that we process the most constrained stream edge groups first,
    // which is crucial for intermediate stream edges that are members of multiple
    // stream edge groups. For instance, if we have the following groups of stream
    // edges (partition counts in parentheses, question marks for intermediate streams):
    //
    //    a. e1 (16), e2 (16)
    //    b. e2 (16), e3 (?)
    //    c. e3 (?), e4 (?)
    //
    // processing them in the above order (most constrained first) is guaranteed to yield
    // correct assignment of partition counts of e3 and e4, i.e. 16, in a single scan.
    //
    // Second, set partition counts for intermediate streams, disregarding groups composed entirely
    // of input streams, i.e. whose streams all have set partitions.

    joinedStreamSets.stream()
        .sorted(Comparator.comparingInt(e -> e.getCategory().getSortOrder()))
        .filter(streamSet -> streamSet.getCategory() != StreamSetCategory.ALL_PARTITION_COUNT_SET)
        .forEach(IntermediateStreamManager::setJoinedStreamPartitions);

    // Set partition count of intermediate streams not participating in joins
    setIntermediateStreamPartitions(jobGraph);

    // Validate partition counts were assigned for all intermediate streams
    validateIntermediateStreamPartitions(jobGraph);
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
   * Set partition counts for intermediate streams in the supplied {@code streamSet}.
   */
  private static void setJoinedStreamPartitions(StreamSet streamSet) {
    Set<StreamEdge> streamEdges = streamSet.getStreamEdges();
    StreamEdge firstStreamWithSetPartitions =
        streamEdges.stream()
            .filter(streamEdge -> streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN)
            .findFirst()
            .orElse(null);

    // This group consists exclusively of intermediate streams with unknown partition counts.
    // We cannot set partition counts for such streams right here but they are tackled later.
    if (firstStreamWithSetPartitions == null) {
      return;
    }

    // Assign partition count to all intermediate stream edges in this set.
    int partitions = firstStreamWithSetPartitions.getPartitionCount();
    for (StreamEdge streamEdge : streamEdges) {
      if (streamEdge.getPartitionCount() == StreamEdge.PARTITIONS_UNKNOWN) {
        streamEdge.setPartitionCount(partitions);
        log.info("Inferred the partition count {} for the join operator {} from {}.",
            new Object[] {partitions, streamSet.getSetId(), firstStreamWithSetPartitions.getName()});
      }
    }
  }

  /* package private */ static int maxPartitions(Collection<StreamEdge> edges) {
    return edges.stream().mapToInt(StreamEdge::getPartitionCount).max().orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }
}
