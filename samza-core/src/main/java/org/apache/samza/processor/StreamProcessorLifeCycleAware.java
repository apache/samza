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

/**
 * This class listens to the life cycle events in a {@link StreamProcessor},
 * and triggers the corresponding callbacks.
 *
 * TODO: right now the callbacks happen during the container life cycle.
 * We need to switch to the real StreamProcessor life cycle.
 */
public interface StreamProcessorLifeCycleAware {
  /**
   * Callback when the Samza processor is started
   */
  void onStart();

  /**
   * Callback when the Samza processor is shut down.
   */
  void onShutdown();

  /**
   * Callback when the Samza processor fails
   * @param t exception of the failure
   */
  void onFailure(Throwable t);

}
