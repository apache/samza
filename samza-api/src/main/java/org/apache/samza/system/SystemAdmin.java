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

package org.apache.samza.system;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Helper interface attached to an underlying system to fetch information about
 * streams, partitions, offsets, etc. This interface is useful for providing
 * utility methods that Samza needs in order to interact with a system.
 */
public interface SystemAdmin {

  /*
   * Start this SystemAdmin
   */
  default void start() {};

  /*
   * Stop this SystemAdmin
   */
  default void stop() {};

  /**
   * Fetches the offsets for the messages immediately after the supplied offsets
   * for a group of SystemStreamPartitions.
   *
   * @param offsets
   *          Map from SystemStreamPartition to current offsets.
   * @return Map from SystemStreamPartition to offsets immediately after the
   *         current offsets.
   */
  Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets);

  /**
   * Fetch metadata from a system for a set of streams.
   *
   * @param streamNames
   *          The streams to to fetch metadata for.
   * @return A map from stream name to SystemStreamMetadata for each stream
   *         requested in the parameter set.
   */
  Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames);

  /**
   * Fetch metadata from a system for a set of SSPs.
   * Implementors should override this if there is a more efficient implementation than delegating to
   * {@link #getSystemStreamMetadata}.
   *
   * @param ssps SSPs for which to get metadata
   * @return A map from SystemStreamPartition to the SystemStreamPartitionMetadata, with an entry for each SSP in
   * {@code ssps} for which metadata could be found
   * @throws RuntimeException if there was an error fetching metadata
   */
  default Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> getSSPMetadata(
      Set<SystemStreamPartition> ssps) {
    Set<String> streams = ssps.stream().map(SystemStream::getStream).collect(Collectors.toSet());
    Map<String, SystemStreamMetadata> streamToSystemStreamMetadata = getSystemStreamMetadata(streams);
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> sspToSSPMetadata = new HashMap<>();
    for (SystemStreamPartition ssp : ssps) {
      SystemStreamMetadata systemStreamMetadata = streamToSystemStreamMetadata.get(ssp.getStream());
      if (systemStreamMetadata != null) {
        SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata =
            systemStreamMetadata.getSystemStreamPartitionMetadata().get(ssp.getPartition());
        if (sspMetadata != null) {
          sspToSSPMetadata.put(ssp, sspMetadata);
        }
      }
    }
    return sspToSSPMetadata;
  }

  /**
   * Compare the two offsets. -1, 0, +1 means offset1 &lt; offset2,
   * offset1 == offset2 and offset1 &gt; offset2 respectively. Return
   * null if those two offsets are not comparable
   *
   * @param offset1 First offset for comparison.
   * @param offset2 Second offset for comparison.
   * @return -1 if offset1 &lt; offset2; 0 if offset1 == offset2; 1 if offset1 &gt; offset2. Null if not comparable
   */
  Integer offsetComparator(String offset1, String offset2);

  /**
   * Create a stream described by the spec.
   *
   * @param streamSpec  The spec, or blueprint from which the physical stream will be created on the system.
   * @return            {@code true} if the stream was actually created and not pre-existing.
   *                    {@code false} if the stream was pre-existing.
   *                    A RuntimeException will be thrown if creation fails.
   */
  default boolean createStream(StreamSpec streamSpec) {
    throw new UnsupportedOperationException();
  }

  /**
   * Validates the stream described by the streamSpec on the system.
   * A {@link StreamValidationException} should be thrown for any validation error.
   *
   * @param streamSpec  The spec, or blueprint for the physical stream on the system.
   * @throws StreamValidationException if validation fails.
   */
  default void validateStream(StreamSpec streamSpec) throws StreamValidationException {
    throw new UnsupportedOperationException();
  }

  /**
   * Clear the entire stream described by the spec.
   * @param streamSpec  The spec for the physical stream on the system.
   * @return {@code true} if the stream was successfully cleared.
   *         {@code false} if clearing stream failed.
   */
  default boolean clearStream(StreamSpec streamSpec) {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete records up to (and including) the provided ssp offsets for all system stream partitions specified in the map
   *
   * @param offsets a map from system stream partition to offset
   */
  default void deleteMessages(Map<SystemStreamPartition, String> offsets) {

  }

}
