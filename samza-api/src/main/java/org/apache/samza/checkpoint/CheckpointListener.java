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

/**
 * SystemConsumers that want to be notified about checkpoints for their SSPs and potentially modify them may
 * implement this interface.
 *
 * Note: For broadcast streams, different Tasks may checkpoint the same ssp with different offsets. It is the
 * SystemConsumer's responsibility to select the appropriate (e.g., lowest) offset for its own use.
 */
public interface CheckpointListener {
  /**
   * Called before writing the checkpoint for each Task in the Container processing SSPs on this system.
   * Implementations may modify the Task offsets to be checkpointed, or return the provided offsets as-is.
   *
   * @param offsets offsets about to be checkpointed.
   * @return potentially modified offsets to be checkpointed.
   */
  default Map<SystemStreamPartition, String> beforeCheckpoint(Map<SystemStreamPartition, String> offsets) {
    return offsets;
  }

  /**
   * Called after writing the checkpoint for each Task in the Container processing SSPs on this system.
   *
   * @param offsets offsets that were checkpointed.
   */
  default void afterCheckpoint(Map<SystemStreamPartition, String> offsets) { }

  /**
   * Called after writing the checkpoint for each Task in the Container processing SSPs on this system.
   * This method is deprecated. Implement {@link #afterCheckpoint(Map)} instead.
   *
   * @param offsets offsets that were checkpointed.
   */
  @Deprecated
  default void onCheckpoint(Map<SystemStreamPartition, String> offsets) {
    afterCheckpoint(offsets);
  }
}
