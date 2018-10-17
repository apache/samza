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
package org.apache.samza.application;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.descriptors.ApplicationDescriptor;


/**
 * A {@link SamzaApplication} describes the inputs, outputs, state, configuration and the logic
 * for processing data from one or more streaming sources.
 * <p>
 * This is the base {@link SamzaApplication}. Implement a {@link StreamApplication} to describe the
 * processing logic using Samza's High Level API in terms of {@link org.apache.samza.operators.MessageStream}
 * operators, or a {@link TaskApplication} to describe it using Samza's Low Level API in terms of per-message
 * processing logic.
 * <p>
 * A {@link SamzaApplication} implementation must have a no-argument constructor, which will be used by the framework
 * to create new instances and call {@link #describe(ApplicationDescriptor)}.
 * <p>
 * Per container context may be managed using {@link org.apache.samza.context.ApplicationContainerContext} and
 * set using {@link ApplicationDescriptor#withApplicationContainerContextFactory}. Similarly, per task context
 * may be managed using {@link org.apache.samza.context.ApplicationTaskContextFactory} and set using
 * {@link ApplicationDescriptor#withApplicationTaskContextFactory}.
 */
@InterfaceStability.Evolving
public interface SamzaApplication<S extends ApplicationDescriptor> {

  /**
   * Describes the inputs, outputs, state, configuration and processing logic using the provided {@code appDescriptor}.
   *
   * @param appDescriptor the {@link ApplicationDescriptor} to use for describing the application.
   */
  void describe(S appDescriptor);
}
