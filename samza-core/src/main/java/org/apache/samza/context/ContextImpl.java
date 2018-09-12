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

public class ContextImpl implements Context {
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final TaskContext taskContext;
  private final ApplicationContainerContext applicationContainerContext;
  private final ApplicationTaskContext applicationTaskContext;

  /**
   * @param jobContext non-null job context
   * @param containerContext non-null framework container context
   * @param taskContext non-null framework task context
   * @param applicationContainerContext nullable application-defined container context
   * @param applicationTaskContext nullable application-defined task context
   */
  public ContextImpl(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext,
      ApplicationContainerContext applicationContainerContext, ApplicationTaskContext applicationTaskContext) {
    this.jobContext = jobContext;
    this.containerContext = containerContext;
    this.taskContext = taskContext;
    this.applicationContainerContext = applicationContainerContext;
    this.applicationTaskContext = applicationTaskContext;
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
    if (this.applicationContainerContext == null) {
      throw new IllegalStateException("No application-defined container context exists");
    }
    return this.applicationContainerContext;
  }

  @Override
  public ApplicationTaskContext getApplicationTaskContext() {
    if (this.applicationTaskContext == null) {
      throw new IllegalStateException("No application-defined task context exists");
    }
    return this.applicationTaskContext;
  }
}
