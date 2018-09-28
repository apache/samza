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
 * An application should implement this if it has a {@link ApplicationContainerContext} that is needed for
 * initialization.
 * <p>
 * This will be called to create an instance of {@link ApplicationContainerContext} during the container initialization
 * stage. At that stage, the framework-provided job-level and container-level contexts are available for creating the
 * {@link ApplicationContainerContext}.
 * <p>
 * This is {@link Serializable} because it is specified in {@link org.apache.samza.application.ApplicationDescriptor}.
 * @param <T> concrete type of {@link ApplicationContainerContext} returned by this factory
 */
public interface ApplicationContainerContextFactory<T extends ApplicationContainerContext> extends Serializable {
  /**
   * Create an instance of the application-defined {@link ApplicationContainerContext}.
   *
   * @param jobContext framework-provided job context used for building {@link ApplicationContainerContext}
   * @param containerContext framework-provided container context used for building {@link ApplicationContainerContext}
   * @return new instance of the application-defined {@link ApplicationContainerContext}
   */
  T create(JobContext jobContext, ContainerContext containerContext);
}
