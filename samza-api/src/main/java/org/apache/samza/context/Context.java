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
 * Container object for all context provided to instantiate an application at runtime.
 */
public interface Context {
  /**
   * Returns the framework-provided context for the overall job that is being run.
   * @return framework-provided job context
   */
  JobContext getJobContext();

  /**
   * Returns the framework-provided context for the container that this is in.
   * <p>
   * Note that this is not the application-defined container context. Use
   * {@link Context#getApplicationContainerContext()} to get the application-defined container context.
   * @return framework-provided container context
   */
  ContainerContext getContainerContext();

  /**
   * Returns the framework-provided context for the task that that this is in.
   * <p>
   * Note that this is not the application-defined task context. Use {@link Context#getApplicationTaskContext()}
   * to get the application-defined task context.
   * @return framework-provided task context
   */
  TaskContext getTaskContext();

  /**
   * Returns the application-defined container context object specified by the
   * {@link ApplicationContainerContextFactory}. This is shared across all tasks in the container, but not across
   * containers.
   * <p>
   * In order to use this in application code, it should be casted to the concrete type that corresponds to the
   * {@link ApplicationContainerContextFactory}.
   * <p>
   * Note that this is not the framework-provided container context. Use {@link Context#getContainerContext()} to get
   * the framework-provided container context.
   * @return application-defined container context
   * @throws IllegalStateException if no context could be built (e.g. no factory provided)
   */
  ApplicationContainerContext getApplicationContainerContext();

  /**
   * Returns the application-defined task context object specified by the {@link ApplicationTaskContextFactory}.
   * Each task will have a separate instance of this.
   * <p>
   * In order to use this in application code, it should be casted to the concrete type that corresponds to the
   * {@link ApplicationTaskContextFactory}.
   * <p>
   * Note that this is not the framework-provided task context. Use {@link Context#getTaskContext()} to get the
   * framework-provided task context.
   * @return application-defined task context
   * @throws IllegalStateException if no context could be built (e.g. no factory provided)
   */
  ApplicationTaskContext getApplicationTaskContext();
}
