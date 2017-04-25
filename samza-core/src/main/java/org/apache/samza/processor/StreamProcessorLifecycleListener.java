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

package org.apache.samza.processor;

import org.apache.samza.annotation.InterfaceStability;


/**
 * This class listens to the life cycle events in a {@link StreamProcessor},
 * and triggers the corresponding callbacks.
 */

@InterfaceStability.Evolving
public interface StreamProcessorLifecycleListener {
  /**
   * Callback when the {@link StreamProcessor} is started
   * This callback is invoke only when {@link org.apache.samza.container.SamzaContainer} starts for the first time in
   * the {@link StreamProcessor}. When there is a re-balance of tasks/partitions among the processors, the container may
   * temporarily be "paused" and re-started again. For such re-starts, this callback is NOT invoked.
   */
  void onStart();

  /**
   * Callback when the {@link StreamProcessor} is shut down.
   */
  void onShutdown();

  /**
   * Callback when the {@link StreamProcessor} fails
   * @param t Cause of the failure
   */
  void onFailure(Throwable t);

}
