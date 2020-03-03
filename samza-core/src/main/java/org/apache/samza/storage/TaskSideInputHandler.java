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

package org.apache.samza.storage;


import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskCallbackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO pick a better name for this class?

/**
 * This class encapsulates all processing logic / state for all side input SSPs within a task.
 */
public class TaskSideInputHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSideInputHandler.class);

  public TaskSideInputHandler(
      TaskName taskName,
      TaskSideInputStorageManager taskSideInputStorageManager,
      Map<String, Set<SystemStreamPartition>> storeToSSPs,
      Map<String, SideInputsProcessor> storeToProcessor,
      SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache,
      CountDownLatch sideInputTasksCaughtUp,
      Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> initialSSPMetadata,
      Map<SystemStreamPartition, Object> sspLockObjects,
      Map<SystemStreamPartition, Optional<String>> checkpointOffsets) {

  }

  public void process(IncomingMessageEnvelope envelope, TaskCallbackFactory callbackFactory) {

  }

  public String getStartingOffset(SystemStreamPartition ssp) {
    return null;
  }

  public String getLastProcessedOffset(SystemStreamPartition ssp) {
    return null;
  }

  public void stop() {

  }

  /**
   * Checks if whether the given offset for the SSP has reached the latest offset (determined at class construction time),
   * removing it from the list of SSPs to catch up. Once the set of SSPs to catch up becomes empty, the latch shared
   * with {@link ContainerStorageManager} will be counted down exactly once.
   *
   * @param ssp The SSP to be checked
   * @param currentOffset The offset to be checked
   * @param isStartingOffset Indicates whether the offset being checked is the starting offset of the SSP (and thus has
   *                         not yet been processed). This will be set to true when each SSP's starting offset is checked
   *                         at startup.
   */
  private synchronized void checkCaughtUp(SystemStreamPartition ssp, String currentOffset, boolean isStartingOffset) {

  }

  /**
   * Gets the starting offsets for the {@link SystemStreamPartition}s belonging to all the side input stores.
   * If the local file offset is available and is greater than the oldest available offset from source, uses it,
   * else falls back to oldest offset in the source.
   *
   * @param fileOffsets offsets from the local offset file
   * @param oldestOffsets oldest offsets from the source
   * @return a {@link Map} of {@link SystemStreamPartition} to offset
   */
  @VisibleForTesting
  Map<SystemStreamPartition, String> getStartingOffsets(
      Map<SystemStreamPartition, String> fileOffsets, Map<SystemStreamPartition, String> oldestOffsets) {
    return new HashMap<>();
  }

  /**
   * Gets the oldest offset for the {@link SystemStreamPartition}s associated with all the store side inputs.
   *   1. Groups the list of the SSPs based on system stream
   *   2. Fetches the {@link SystemStreamMetadata} from {@link StreamMetadataCache}
   *   3. Fetches the partition metadata for each system stream and fetch the corresponding partition metadata
   *      and populates the oldest offset for SSPs belonging to the system stream.
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to their oldest offset.
   */
  @VisibleForTesting
  Map<SystemStreamPartition, String> getOldestOffsets() {
    return new HashMap<>();
  }
}
