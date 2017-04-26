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
package org.apache.samza;


/**
 *                                                                   runloop completed [OR]
 *                  container.run()           runloop.run            container.shutdown()
 *    NOT_STARTED -----------------> STARTING ------------> RUNNING -----------------------> STOPPED
 *                                       |      Error in runloop |
 *                                       |      [OR] Error when  |
 *                           Error when  |      stopping         |
 *                   starting components |      components       |
 *                                       V                       |
 *                                    FAILED <-------------------|
 *
 */

/**
 * Indicates the current status of a {@link org.apache.samza.container.SamzaContainer}
 */
public enum  SamzaContainerStatus {
  /**
   * Indicates that the container has not been started
   */
  NOT_STARTED,

  /**
   * Indicates that the container is starting all the components required by the
   * {@link org.apache.samza.container.RunLoop} for processing
   */
  STARTING,

  /**
   * Indicates that the container started the {@link org.apache.samza.container.RunLoop}
   */
  RUNNING,

  /**
   * Indicates that the container was successfully stopped either due to task-initiated shutdown
   * (eg. end-of-stream triggered shutdown or application-driven shutdown of all tasks and hence, the container) or
   * due to external shutdown requests (eg. from {@link org.apache.samza.processor.StreamProcessor})
   */
  STOPPED,

  /**
   * Indicates that the container failed during any of its 3 active states -
   * {@link #STARTING}, {@link #RUNNING}, {@link #STOPPED}
   */
  FAILED
}
