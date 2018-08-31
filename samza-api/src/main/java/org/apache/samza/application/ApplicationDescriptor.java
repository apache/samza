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
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;


/**
 * The base interface class to describe a user application in Samza.
 * <p>
 * Sub-classes {@link StreamAppDescriptor} and {@link TaskAppDescriptor} are specific interfaces for applications written
 * in high-level DAG and low-level task APIs, respectively.
 *
 * @param <S> sub-class of user application descriptor. It has to be either {@link StreamAppDescriptor} or
 *            {@link TaskAppDescriptor}
 */
@InterfaceStability.Evolving
public interface ApplicationDescriptor<S extends ApplicationDescriptor> {

  /**
   * Get {@link Config}
   * @return config object
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
   * Sets the default SystemDescriptor to use for intermediate streams. This is equivalent to setting
   * {@code job.default.system} and its properties in configuration.
   * <p>
   * If the default system descriptor is set, it must be set <b>before</b> creating any input/output/intermediate streams.
   * <p>
   * If an input/output stream is created with a stream-level Serde, they will be used, else the serde specified
   * for the {@code job.default.system} in configuration will be used.
   * <p>
   * Providing an incompatible message type for the intermediate streams that use the default serde will result in
   * {@link ClassCastException}s at runtime.
   *
   * @param defaultSystemDescriptor the default system descriptor to use
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code defaultSystemDescriptor} set as its default system
   */
  S withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor);

  /**
   * Sets a set of customized {@link MetricsReporter}s in the application
   *
   * @param reporterFactories the map of customized {@link MetricsReporterFactory} objects to be used
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code reporterFactories}
   */
  S withMetricsReporterFactories(Map<String, MetricsReporterFactory> reporterFactories);

}
