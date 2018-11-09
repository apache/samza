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
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;


/**
 * The factory for creating {@link ApplicationContainerContext} instances for a {@link SamzaApplication} during
 * container initialization.
 * <p>
 * Use {@link ApplicationDescriptor#withApplicationContainerContextFactory} to provide the
 * {@link ApplicationContainerContextFactory}. Use {@link Context#getApplicationContainerContext()} to get the created
 * {@link ApplicationContainerContext} instance for the current container.
 * <p>
 * The {@link ApplicationContainerContextFactory} implementation must be {@link Serializable}.
 *
 * @param <T> concrete type of {@link ApplicationContainerContext} created by this factory
 */
public interface ApplicationContainerContextFactory<T extends ApplicationContainerContext> extends Serializable {

  /**
   * Creates an instance of the application-defined {@link ApplicationContainerContext}.
   *
   * @param jobContext framework-provided job context
   * @param containerContext framework-provided container context
   * @return a new instance of the application-defined {@link ApplicationContainerContext}
   */
  T create(JobContext jobContext, ContainerContext containerContext);
}
