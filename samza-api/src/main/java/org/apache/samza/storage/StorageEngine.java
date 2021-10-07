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


import java.nio.file.Path;
import java.util.Optional;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.context.Context;
import org.apache.samza.system.ChangelogSSPIterator;

/**
 * A storage engine for managing state maintained by a stream processor.
 *
 * <p>
 * This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 * </p>
 */
public interface StorageEngine {

  /**
   * Use for lifecycle management for StorageEngine, ContainerStorageManager after restoring each store issues
   * init on each configured storage engine
   * @param context holder for all application and framework defined objects
   */
  void init(Context context);

  /**
   * Restore the content of this StorageEngine from the changelog. Messages are
   * provided in one {@link java.util.Iterator} and not deserialized for
   * efficiency, allowing the implementation to optimize replay, if possible.
   *
   * The implementers are expected to handle interrupt signals to the restoration thread and rethrow the exception to
   * upstream so that {@code TaskRestoreManager} can accordingly notify the container.
   *
   * @param envelopes
   *          An iterator of envelopes that the storage engine can read from to
   *          restore its state on startup.
   * @throws InterruptedException when received interrupts during restoration
   */
  void restore(ChangelogSSPIterator envelopes) throws InterruptedException;

  /**
   * Flush any cached messages
   */
  void flush();

  /**
   * Checkpoint store snapshots.
   */
  @InterfaceStability.Unstable
  Optional<Path> checkpoint(CheckpointId id);

  /**
   * Close the storage engine
   */
  void stop();

  /**
   * Get store properties
   *
   * @return store properties
   */
  StoreProperties getStoreProperties();
}
