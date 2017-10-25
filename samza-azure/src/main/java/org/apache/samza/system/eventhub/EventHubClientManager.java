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

package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventHubClient;

/**
 * <p>
 * EventHubClient manager is the interface that must be implemented to wrap the
 * {@link EventHubClient} with lifecycle hooks for initialization and close.
 * </p>
 *
 * <p>
 * {@link #init()} should be invoked once during the startup and provides a
 * hook to perform some initialization before the creation of the underlying
 * {@link EventHubClient}. {@link #close(long)} is invoked once during shut-down
 * and can be used to perform clean-ups.
 * </p>
 */
public interface EventHubClientManager {
  /**
   * A constant that can be used in the close method's timeout parameter to
   * denote that the close invocation should block until all the teardown
   * operations for the {@link EventHubClient} are completed
   */
  public static int BLOCK_UNTIL_CLOSE = -1;

  /**
   * Lifecycle hook to perform initializations for the creation of
   * the underlying {@link EventHubClient}.
   */
  void init();

  /**
   * Returns the underlying {@link EventHubClient} instance. Multiple invocations
   * of this method should return the same instance instead of
   * creating new ones.
   *
   * @return EventHub client instance of the wrapper
   */
  EventHubClient getEventHubClient();

  /**
   * Tries to close the {@link EventHubClient} instance within the provided
   * timeout. Use this method to perform clean-ups after the execution of the
   * {@link EventHubClient}. Set timeout the {@link #BLOCK_UNTIL_CLOSE} to
   * block until the client is closed.
   *
   * @param timeoutMs Close timeout in Milliseconds
   */
  void close(long timeoutMs);
}
