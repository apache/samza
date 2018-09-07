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

import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;


/**
 * The interface class to describe the configuration, input and output streams, and processing logic in a {@link SamzaApplication}.
 * <p>
 * Sub-classes {@link StreamApplicationDescriptor} and {@link TaskApplicationDescriptor} are specific interfaces for applications
 * written in high-level {@link StreamApplication} and low-level {@link TaskApplication} APIs, respectively.
 *
 * @param <S> sub-class of user application descriptor.
 */
@InterfaceStability.Evolving
public interface ApplicationDescriptor<S extends ApplicationDescriptor> {

  /**
   * Get the {@link Config} of the application
   * @return config of the application
   */
  Config getConfig();

  /**
   * Sets the {@link ContextManager} for this application.
   * <p>
   * Setting the {@link ContextManager} is optional. The provided {@link ContextManager} can be used to build the shared
   * context between the operator functions within a task instance
   *
   * TODO: this should be replaced by the shared context factory when SAMZA-1714 is fixed.

   * @param contextManager the {@link ContextManager} to use for the application
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code contextManager} set as its {@link ContextManager}
   */
  S withContextManager(ContextManager contextManager);

  /**
   * Sets the {@link ProcessorLifecycleListenerFactory} for this application.
   *
   * <p>Setting a {@link ProcessorLifecycleListenerFactory} is optional to a user application. It allows users to
   * plug in optional code to be invoked in different stages before/after the main processing logic is started/stopped in
   * the application.
   *
   * @param listenerFactory the user implemented {@link ProcessorLifecycleListenerFactory} that creates lifecycle listener
   *                        with callback methods before and after the start/stop of each StreamProcessor in the application
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code listenerFactory} set as its {@link ProcessorLifecycleListenerFactory}
   */
  S withProcessorLifecycleListenerFactory(ProcessorLifecycleListenerFactory listenerFactory);

  /**
   * Sets a set of customized {@link MetricsReporterFactory}s in the application
   *
   * @param reporterFactories the map of customized {@link MetricsReporterFactory}s to be used
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code reporterFactories}
   */
  S withMetricsReporterFactories(Map<String, MetricsReporterFactory> reporterFactories);

}
