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
package org.apache.samza.container;

/**
 * A Listener for {@link SamzaContainer} lifecycle events.
 */
public interface SamzaContainerListener {

  /**
   * Method invoked when the {@link SamzaContainer} state is {@link org.apache.samza.SamzaContainerStatus#NOT_STARTED}
   * and is about to transition to {@link org.apache.samza.SamzaContainerStatus#STARTING} to start the initialization sequence.
   */
  void beforeStart();

  /**
   *  Method invoked after the {@link SamzaContainer} has successfully transitioned to
   *  the {@link org.apache.samza.SamzaContainerStatus#STARTED} state and is about to start the
   *  {@link org.apache.samza.container.RunLoop}
   */
  void afterStart();

  /**
   *  Method invoked after the {@link SamzaContainer} has successfully transitioned to
   *  {@link org.apache.samza.SamzaContainerStatus#STOPPED} state. Details on state transitions can be found in
   *  {@link org.apache.samza.SamzaContainerStatus}
   *  <br>
   *  <b>Note</b>: This will be the last call after completely shutting down the SamzaContainer without any
   *  exceptions/errors.
   */
  void afterStop();

  /**
   *  Method invoked after the {@link SamzaContainer} has  transitioned to
   *  {@link org.apache.samza.SamzaContainerStatus#FAILED} state. Details on state transitions can be found in
   *  {@link org.apache.samza.SamzaContainerStatus}
   *  <br>
   *  <b>Note</b>: {@link #afterFailure(Throwable)} is mutually exclusive to {@link #afterStop()}.
   *  @param t Throwable that caused the container failure.
   */
  void afterFailure(Throwable t);
}
