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
package org.apache.samza.container.host;

/**
 * An object that monitors per-process system statistics like memory utilization and reports this via a
 * {@link SystemStatisticsMonitor.Listener}.
 */
public interface SystemStatisticsMonitor {
  /**
   * Starts the system statistics monitor.
   */
  void start();

  /**
   * Stops the memory usage monitor. Once shutdown is complete listeners will not longer receive
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
   * A listener that is notified when the monitor has sampled a new statistic.
   * Register this listener with {@link #registerListener(Listener)} to receive updates.
   */
  interface Listener {
    /**
     * Invoked with new samples as they become available.
     *
     * @param sample the currently sampled statistic.
     */
    void onUpdate(SystemMemoryStatistics sample);
  }

}
