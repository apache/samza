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

package org.apache.samza.checkpoint;

import java.util.Map;
import org.apache.samza.system.SystemStreamPartition;


public interface Checkpoint {
  /**
   * Gets the version number of the Checkpoint
   * @return Short indicating the version number
   */
  short getVersion();

  /**
   * Gets a unmodifiable view of the current Samza stream offsets.
   * The return value differs based on the Checkpoint version:
   * <ol>
   *    <li>For CheckpointV1 returns the offsets including the input ssp-offsets mapping and
   *      changelog ssp-KafkaStateChangelogOffset</li>
   *    <li>For CheckpointV2 returns the input offsets only.</li>
   * </ol>
   * @return A unmodifiable view of a Map of Samza streams to their recorded offsets.
   */
  Map<SystemStreamPartition, String> getOffsets();
}
