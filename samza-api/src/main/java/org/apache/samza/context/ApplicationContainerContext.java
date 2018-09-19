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
 * An application should implement this to contain any runtime objects required by processing logic which can be shared
 * across all tasks in a container. A single instance of this will be created in each container.
 * <p>
 * This needs to be created by an implementation of {@link ApplicationContainerContextFactory}. The factory should
 * create the runtime objects contained within this context.
 * <p>
 * If it is necessary to have a separate instance per task, then use {@link ApplicationTaskContext} instead.
 * <p>
 * This class does not need to be {@link java.io.Serializable} and instances are not persisted across deployments.
 */
public interface ApplicationContainerContext {
  /**
   * Lifecycle logic which will run after tasks in the container are initialized but before processing begins.
   * <p>
   * If this throws an exception, then the container will fail to start.
   */
  void start();

  /**
   * Lifecycle logic which will run after processing ends but before tasks in the container are closed.
   * <p>
   * If this throws an exception, then the container will fail to fully shut down.
   */
  void stop();
}
