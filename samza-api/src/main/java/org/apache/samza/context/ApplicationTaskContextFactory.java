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

import java.io.Serializable;


/**
 * The factory for creating {@link ApplicationTaskContext} instances for a
 * {@link org.apache.samza.application.SamzaApplication}during task initialization.
 * <p>
 * Use {@link org.apache.samza.application.descriptors.ApplicationDescriptor#withApplicationTaskContextFactory} to
 * provide the {@link ApplicationTaskContextFactory}. Use {@link Context#getApplicationTaskContext()} to
 * get the created {@link ApplicationTaskContext} instance for the current task.
 * <p>
 * The {@link ApplicationTaskContextFactory} implementation must be {@link Serializable}.
 *
 * @param <T> concrete type of {@link ApplicationTaskContext} created by this factory
 */
public interface ApplicationTaskContextFactory<T extends ApplicationTaskContext> extends Serializable {

  /**
   * Creates an instance of the application-defined {@link ApplicationTaskContext}.
   *
   * @param jobContext framework-provided job context
   * @param containerContext framework-provided container context
   * @param taskContext framework-provided task context
   * @param applicationContainerContext application-defined container context
   * @return a new instance of the application-defined {@link ApplicationTaskContext}
   */
  T create(JobContext jobContext, ContainerContext containerContext, TaskContext taskContext,
      ApplicationContainerContext applicationContainerContext);
}
