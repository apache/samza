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

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;


public class ContextImpl implements Context {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;
  private final Optional<ApplicationContainerContext> applicationContainerContextOptional;
  private final Optional<ApplicationTaskContext> applicationTaskContextOptional;

  /**
   * @param jobContext non-null job context
   * @param containerContext non-null framework container context
   * @param taskContext non-null framework task context
   * @param applicationContainerContextOptional optional application-defined container context
   * @param applicationTaskContextOptional optional application-defined task context
   */
  public ContextImpl(JobContext jobContext,
      ContainerContext containerContext,
      TaskContext taskContext,
      Optional<ApplicationContainerContext> applicationContainerContextOptional,
      Optional<ApplicationTaskContext> applicationTaskContextOptional) {
    this.jobContext = Preconditions.checkNotNull(jobContext, "Job context can not be null");
    this.containerContext = Preconditions.checkNotNull(containerContext, "Container context can not be null");
    this.taskContext = Preconditions.checkNotNull(taskContext, "Task context can not be null");
    this.applicationContainerContextOptional = applicationContainerContextOptional;
    this.applicationTaskContextOptional = applicationTaskContextOptional;
  }

  @Override
  public JobContext getJobContext() {
    return this.jobContext;
  }

  @Override
  public ContainerContext getContainerContext() {
    return this.containerContext;
  }

  @Override
  public TaskContext getTaskContext() {
    return this.taskContext;
  }

  @Override
  public ApplicationContainerContext getApplicationContainerContext() {
    if (!this.applicationContainerContextOptional.isPresent()) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return this.applicationContainerContextOptional.get();
  }

  @Override
  public ApplicationTaskContext getApplicationTaskContext() {
    if (!this.applicationTaskContextOptional.isPresent()) {
      throw new IllegalStateException("No application-defined task context exists");
    }
    return this.applicationTaskContextOptional.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContextImpl context = (ContextImpl) o;
    return Objects.equals(jobContext, context.jobContext) && Objects.equals(containerContext, context.containerContext)
        && Objects.equals(taskContext, context.taskContext) && Objects.equals(applicationContainerContextOptional,
        context.applicationContainerContextOptional) && Objects.equals(applicationTaskContextOptional,
        context.applicationTaskContextOptional);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobContext, containerContext, taskContext, applicationContainerContextOptional,
        applicationTaskContextOptional);
  }
}
