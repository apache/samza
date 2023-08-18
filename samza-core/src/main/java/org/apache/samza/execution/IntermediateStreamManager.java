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
import java.util.Optional;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ApplicationConfig.ApplicationMode;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.execution.ExecutionPlanner.StreamSet;


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
   * Calculates the number of partitions of all intermediate streams
   */
  /* package private */ void calculatePartitions(JobGraph jobGraph, Collection<StreamSet> joinedStreamSets) {

    // Set partition count of intermediate streams participating in joins
    setJoinedIntermediateStreamPartitions(joinedStreamSets);

    // Set partition count of intermediate streams not participating in joins
    setIntermediateStreamPartitions(jobGraph);

    // Validate partition counts were assigned for all intermediate streams
    validateIntermediateStreamPartitions(jobGraph);
  }

  /**
   * Sets partition counts of intermediate streams which have not been assigned partition counts.
   */
  @VisibleForTesting
  void setIntermediateStreamPartitions(JobGraph jobGraph) {
    final String defaultPartitionsConfigProperty = JobConfig.JOB_INTERMEDIATE_STREAM_PARTITIONS;
    int partitions = config.getInt(defaultPartitionsConfigProperty, StreamEdge.PARTITIONS_UNKNOWN);
    if (partitions == StreamEdge.PARTITIONS_UNKNOWN) {
      // use the following simple algo to figure out the partitions
      // partition = MAX(MAX(Input topic partitions), MAX(Output topic partitions))
      // partition will be further bounded by MAX_INFERRED_PARTITIONS.
      // This is important when running in hadoop where an HDFS input can have lots of files (partitions).
      int maxInPartitions = maxPartitions(jobGraph.getInputStreams());
      int maxOutPartitions = maxPartitions(jobGraph.getOutputStreams());
      partitions = Math.max(maxInPartitions, maxOutPartitions);

      ApplicationMode applicationMode = getAppMode();
      if (partitions > MAX_INFERRED_PARTITIONS && ApplicationMode.BATCH.equals(applicationMode)) {
        partitions = MAX_INFERRED_PARTITIONS;
        log.warn(String.format("Inferred intermediate stream partition count %d is greater than the max %d. Using the max.",
            partitions, MAX_INFERRED_PARTITIONS));
      }

      log.info("Using {} as the default partition count for intermediate streams", partitions);
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

  @VisibleForTesting
  ApplicationMode getAppMode() {
    return new ApplicationConfig(config).getAppMode();
  }

  /**
   * Sets partition counts of intermediate streams participating in joins operations.
   */
  private static void setJoinedIntermediateStreamPartitions(Collection<StreamSet> joinedStreamSets) {
    // Map every intermediate stream to all the stream-sets it appears in
    Multimap<StreamEdge, StreamSet> intermediateStreamToStreamSets = HashMultimap.create();
    for (StreamSet streamSet : joinedStreamSets) {
      for (StreamEdge streamEdge : streamSet.getStreamEdges()) {
        if (streamEdge.getPartitionCount() == StreamEdge.PARTITIONS_UNKNOWN) {
          intermediateStreamToStreamSets.put(streamEdge, streamSet);
        }
      }
    }

    Set<StreamSet> streamSets = new HashSet<>(joinedStreamSets);
    Set<StreamSet> processedStreamSets = new HashSet<>();

    while (!streamSets.isEmpty()) {
      // Retrieve and remove one stream set
      StreamSet streamSet = streamSets.iterator().next();
      streamSets.remove(streamSet);

      // Find any stream with set partitions in this set
      Optional<StreamEdge> streamWithSetPartitions =
          streamSet.getStreamEdges().stream()
              .filter(streamEdge -> streamEdge.getPartitionCount() != StreamEdge.PARTITIONS_UNKNOWN)
              .findAny();

      if (streamWithSetPartitions.isPresent()) {
        // Mark this stream-set as processed since we won't need to re-examine it ever again.
        // It is important that we do this first before processing any intermediate streams
        // that may be in this stream-set.
        processedStreamSets.add(streamSet);

        // Set partitions of all intermediate streams in this set (if any)
        int partitions = streamWithSetPartitions.get().getPartitionCount();
        for (StreamEdge streamEdge : streamSet.getStreamEdges()) {
          if (streamEdge.getPartitionCount() == StreamEdge.PARTITIONS_UNKNOWN) {
            streamEdge.setPartitionCount(partitions);
            // Add all unprocessed stream-sets in which this intermediate stream appears
            Collection<StreamSet> streamSetsIncludingIntStream = intermediateStreamToStreamSets.get(streamEdge);
            streamSetsIncludingIntStream.stream()
                .filter(s -> !processedStreamSets.contains(s))
                .forEach(streamSets::add);
          }
        }
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

  /* package private */ static int maxPartitions(Collection<StreamEdge> edges) {
    return edges.stream().mapToInt(StreamEdge::getPartitionCount).max().orElse(StreamEdge.PARTITIONS_UNKNOWN);
  }
}
