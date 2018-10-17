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
package org.apache.samza.context;


/**
 * An {@link ApplicationContainerContext} instance can be used for holding per-container runtime state and objects
 * and managing their lifecycle. This context is shared across all tasks in the container.
 * <p>
 * Use {@link org.apache.samza.application.descriptors.ApplicationDescriptor#withApplicationContainerContextFactory}
 * to provide the {@link ApplicationContainerContextFactory}. Use {@link Context#getApplicationContainerContext()} to
 * get the created {@link ApplicationContainerContext} instance for the current container.
 * <p>
 * A unique instance of {@link ApplicationContainerContext} is created in each container. If the
 * container moves or the container model changes (e.g. due to failure or re-balancing), a new instance is created.
 * <p>
 * Use the {@link ApplicationContainerContextFactory} to create any runtime state and objects, and the
 * {@link ApplicationContainerContext#start()} and {@link ApplicationContainerContext#stop()} methods to
 * manage their lifecycle.
 * <p>
 * Use {@link ApplicationTaskContext} to hold unique runtime state and objects for each task within a container.
 * <p>
 * Unlike its {@link ApplicationContainerContextFactory}, an implementation does not need to be
 * {@link java.io.Serializable}.
 */
public interface ApplicationContainerContext {
  /**
   * Starts this {@link ApplicationContainerContext} after all tasks in the container are initialized but before
   * processing begins.
   * <p>
   * If this throws an exception, the container will fail to start.
   */
  void start();

  /**
   * Stops this {@link ApplicationContainerContext} after processing ends bue before any tasks in the container
   * are closed.
   * <p>
   * If this throws an exception, the container will fail to fully shut down.
   */
  void stop();
}
