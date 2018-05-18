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

package org.apache.samza.system.inmemory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Initial draft of in-memory {@link SystemAdmin}. It is test only and not meant for production use right now.
 */
public class InMemorySystemAdmin implements SystemAdmin {
  private final InMemoryManager inMemoryManager;

  public InMemorySystemAdmin(InMemoryManager manager) {
    inMemoryManager = manager;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  /**
   * Fetches the offsets for the messages immediately after the supplied offsets
   * for a group of SystemStreamPartitions.
   *
   * @param offsets
   *          Map from SystemStreamPartition to current offsets.
   * @return Map from SystemStreamPartition to offsets immediately after the
   *         current offsets.
   */
  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    return offsets.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            String offset = entry.getValue();
            return String.valueOf(Integer.valueOf(offset) + 1);
          }));
  }

  /**
   * Fetch metadata from a system for a set of streams.
   *
   * @param streamNames
   *          The streams to to fetch metadata for.
   * @return A map from stream name to SystemStreamMetadata for each stream
   *         requested in the parameter set.
   */
  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    return inMemoryManager.getSystemStreamMetadata(streamNames);
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
  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (offset1 == null || offset2 == null) {
      return null;
    }

    return Integer.compare(Integer.parseInt(offset1), Integer.parseInt(offset2));
  }

  /**
   * Create a stream described by the spec.
   *
   * @param streamSpec  The spec, or blueprint from which the physical stream will be created on the system.
   * @return            {@code true} if the stream was actually created and not pre-existing.
   *                    {@code false} if the stream was pre-existing.
   *                    A RuntimeException will be thrown if creation fails.
   */
  @Override
  public boolean createStream(StreamSpec streamSpec) {
    return inMemoryManager.initializeStream(streamSpec);
  }

  /**
   * Validates the stream described by the streamSpec on the system.
   * A {@link StreamValidationException} should be thrown for any validation error.
   *
   * @param streamSpec  The spec, or blueprint for the physical stream on the system.
   * @throws StreamValidationException if validation fails.
   */
  @Override
  public void validateStream(StreamSpec streamSpec) throws StreamValidationException {

  }

  /**
   * Clear the stream described by the spec.
   * @param streamSpec  The spec for the physical stream on the system.
   * @return {@code true} if the stream was successfully cleared.
   *         {@code false} if clearing stream failed.
   */
  @Override
  public boolean clearStream(StreamSpec streamSpec) {
    return false;
  }
}
