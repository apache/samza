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

import org.apache.samza.application.descriptors.ApplicationDescriptor;

/**
 * A holder for all framework and application defined contexts at runtime.
 */
public interface Context {
  /**
   * Gets the framework-provided context for the job.
   *
   * @return the framework-provided job context
   */
  JobContext getJobContext();

  /**
   * Gets the framework-provided context for the current container. This context is shared by all tasks within
   * the container.
   * <p>
   * Use {@link #getApplicationContainerContext()} to get the application-defined container context.
   *
   * @return the framework-provided container context
   */
  ContainerContext getContainerContext();

  /**
   * Gets the framework-provided context for the current task.
   * <p>
   * Use {@link #getApplicationTaskContext()} to get the application-defined task context.
   *
   * @return the framework-provided task context
   */
  TaskContext getTaskContext();

  /**
   * Gets the application-defined context for the current container. This context is shared by all tasks within
   * the container.
   * <p>
   * Use {@link ApplicationDescriptor#withApplicationContainerContextFactory} to provide a factory for this context.
   * Cast the returned context to the concrete implementation type to use it.
   * <p>
   * Use {@link #getContainerContext()} to get the framework-provided container context.
   *
   * @return the application-defined container context
   * @throws IllegalStateException if no {@link ApplicationContainerContextFactory} was was provided for the application
   */
  ApplicationContainerContext getApplicationContainerContext();

  /**
   * Gets the application-defined task context for the current task. This context is unique to this task.
   * <p>
   * Use {@link ApplicationDescriptor#withApplicationTaskContextFactory} to provide a factory for this context.
   * Cast the returned context to the concrete implementation type to use it.
   * <p>
   * Use {@link Context#getTaskContext()} to get the framework-provided task context.
   *
   * @return the application-defined task context
   * @throws IllegalStateException if no {@link ApplicationTaskContextFactory} was provided for the application
   */
  ApplicationTaskContext getApplicationTaskContext();

  /**
   * Gets the {@link ExternalContext} that was created outside of the application.
   * <p>
   * Use {@link org.apache.samza.runtime.ApplicationRunner#run(ExternalContext)} to provide this context.
   *
   * @return the external context provided for the application
   */
  ExternalContext getExternalContext();
}
