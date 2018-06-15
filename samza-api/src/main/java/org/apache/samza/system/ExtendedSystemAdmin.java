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

import java.util.Map;
import java.util.Set;

/**
 * Interface extends the more generic SystemAdmin interface
 * TODO: Merge this interface method with SystemAdmin when we upgrade to JDK 1.8
 */
public interface ExtendedSystemAdmin extends SystemAdmin {
  Map<String, SystemStreamMetadata> getSystemStreamPartitionCounts(Set<String> streamNames, long cacheTTL);

  /**
   * Deprecated: Use {@link SystemAdmin#getSSPMetadata}, ideally combined with caching (i.e. SSPMetadataCache).
   * Makes fewer offset requests than getSystemStreamMetadata
   */
  @Deprecated
  String getNewestOffset(SystemStreamPartition ssp, Integer maxRetries);
}
