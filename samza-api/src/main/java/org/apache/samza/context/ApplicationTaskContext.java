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
 * An {@link ApplicationTaskContext} instance can be used for holding per-task runtime state and objects and managing
 * their lifecycle in an {@link org.apache.samza.application.SamzaApplication}
 * <p>
 * Use {@link org.apache.samza.application.descriptors.ApplicationDescriptor#withApplicationTaskContextFactory}
 * to provide the {@link ApplicationTaskContextFactory}. Use {@link Context#getApplicationTaskContext()} to get
 * the created {@link ApplicationTaskContext} instance for the current task.
 * <p>
 * A unique instance of {@link ApplicationTaskContext} is created for each task in a container.
 * Use the {@link ApplicationTaskContextFactory} to create any runtime state and objects, and the
 * {@link ApplicationTaskContext#start()} and {@link ApplicationTaskContext#stop()} methods to manage their lifecycle.
 * <p>
 * Use {@link ApplicationContainerContext} to hold runtime state and objects shared across all tasks within a container.
 * Use {@link TaskContext} to access framework-provided context for a task.
 * <p>
 * Unlike its {@link ApplicationTaskContextFactory}, an implementation does not need to be
 * {@link java.io.Serializable}.
 */
public interface ApplicationTaskContext {

  /**
   * Starts this {@link ApplicationTaskContext} after its task is initialized but before any messages are processed.
   * <p>
   * If this throws an exception, the container will fail to start.
   */
  void start();

  /**
   * Stops this {@link ApplicationTaskContext} after processing ends but before its task is closed.
   * <p>
   * If this throws an exception, the container will fail to fully shut down.
   */
  void stop();
}
