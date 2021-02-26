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

import java.util.Map;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.system.SystemStreamPartition;


/**
 * The helper interface restores task state.
 */
public interface TaskRestoreManager {

  /**
   * Init state resources such as file directories.
   */
  void init(Checkpoint checkpoint);

  /**
   * Restore state from checkpoints, state snapshots and changelog.
   * Currently, store restoration happens on a separate thread pool within {@code ContainerStorageManager}. In case of
   * interrupt/shutdown signals from {@code SamzaContainer}, {@code ContainerStorageManager} may interrupt the restore
   * thread.
   *
   * Note: Typically, interrupt signals don't bubble up as {@link InterruptedException} unless the restore thread is
   * waiting on IO/network. In case of busy looping, implementors are expected to check the interrupt status of the
   * thread periodically and shutdown gracefully before throwing {@link InterruptedException} upstream.
   * {@code SamzaContainer} will not wait for clean up and the interrupt signal is the best effort by the container
   * to notify that its shutting down.
   */
  void restore() throws InterruptedException;

  /**
   * Stop all persistent stores after restoring.
   */
  void stopPersistentStores();

}
