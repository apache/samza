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
package org.apache.samza.container.disk;

/**
 * An object that monitors the amount of disk space used and reports this usage via a
 * {@link DiskSpaceMonitor.Listener}.
 */
public interface DiskSpaceMonitor {
  /**
   * Starts the disk space monitor.
   */
  void start();

  /**
   * Stops the disk space monitor. Once shutdown is complete listeners will not longer receive
   * new samples. A stopped monitor cannot be restarted with {@link #start()}.
   */
  void stop();

  /**
   * Registers the specified listener with this monitor. The listener will be called
   * when the monitor has a new sample. The update interval is implementation specific.
   *
   * @param listener the listener to register
   * @return {@code true} if the registration was successful and {@code false} if not. Registration
   * can fail if the monitor has been stopped or if the listener was already registered.
   */
  boolean registerListener(Listener listener);

  /**
   * A listener that is notified when the disk space manager has sampled a new disk usage value.
   * Register this listener with {@link #registerListener(Listener)} to receive updates.
   */
  interface Listener {
    /**
     * Invoked with new samples as they become available.
     * <p>
     * Updates are guaranteed to be serialized. In other words, a listener's onUpdate callback
     * may only be invoked once at a time by a DiskSpaceMonitor.
     *
     * @param diskUsageSample the measured disk usage size in bytes.
     */
    void onUpdate(long diskUsageSample);
  }
}
