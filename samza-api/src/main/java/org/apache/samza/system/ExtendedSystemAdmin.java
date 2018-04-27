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


/**
 * Interface extends the more generic SystemAdmin interface
 * TODO: Merge this interface method with SystemAdmin when we upgrade to JDK 1.8
 */
public interface ExtendedSystemAdmin extends SystemAdmin {
  Map<String, SystemStreamMetadata> getSystemStreamPartitionCounts(Set<String> streamNames, long cacheTTL);

  /**
   * Deprecated: Use/implement getNewestOffsets instead
   * Gets the newest offset for an {@code ssp}. This makes fewer offset requests than getSystemStreamMetadata.
   * @return Newest offset for ssp. Returns null if no newest offset was found (e.g. topic is empty).
   * @throws RuntimeException if the newest offset information could not be accessed. This exception case is different
   * than the "no newest offset exists" case, because in the "no newest offset exists" case, the newest offset info was
   * accessible, but none existed.
   */
  @Deprecated
  String getNewestOffset(SystemStreamPartition ssp, Integer maxRetries);

  /**
   * Gets the newest offsets for {@code ssps}.
   * <p>
   *   Implementation notes: Override the default implementation if a more efficient batch get exists. It is up to the
   *   implementor to give a best-effort attempt to fetch newest offsets (including internal retries).
   * </p>
   * @return A Map containing newest offsets for each ssp which had a newest offset. Each key is an ssp, and the
   * corresponding value is the newest offset. An ssp which does not have a newest offset (e.g. topic is empty) does not
   * have an entry in the map.
   * @throws RuntimeException if the newest offset information could not be accessed. This exception case is different
   * than the "no newest offset exists" case, because in the "no newest offset exists" case, the newest offset info was
   * accessible, but none existed.
   */
  default Map<SystemStreamPartition, String> getNewestOffsets(Set<SystemStreamPartition> ssps) {
    // default implementation exists for backwards compatibility
    // some fallback retry count since getNewestOffset needs it; remove this when getNewestOffset is removed
    final int defaultRetriesPerSSP = 3;
    final Map<SystemStreamPartition, String> newestOffsets = new HashMap<>();
    for (final SystemStreamPartition ssp: ssps) {
      final String newestOffset = getNewestOffset(ssp, defaultRetriesPerSSP);
      if (newestOffset != null) {
        newestOffsets.put(ssp, newestOffset);
      }
    }
    return newestOffsets;
  }
}
