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
package org.apache.samza.application.descriptors;

import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.system.descriptors.SystemDescriptor;


/**
 * An {@link ApplicationDescriptor} contains the description of inputs, outputs, state, configuration and the
 * processing logic for a {@link org.apache.samza.application.SamzaApplication}.
 * <p>
 * This is the base {@link ApplicationDescriptor} and provides functionality common to all
 * {@link org.apache.samza.application.SamzaApplication}.
 * {@link org.apache.samza.application.StreamApplication#describe} will provide access to a
 * {@link StreamApplicationDescriptor} with additional functionality for describing High Level API applications.
 * Similarly, {@link org.apache.samza.application.TaskApplication#describe} will provide access to a
 * {@link TaskApplicationDescriptor} with additional functionality for describing Low Level API applications.
 * <p>
 * Use the {@link ApplicationDescriptor} to set the container scope context factory using
 * {@link ApplicationDescriptor#withApplicationContainerContextFactory}, and task scope context factory using
 * {@link ApplicationDescriptor#withApplicationTaskContextFactory}. Please note that the terms {@code container}
 * and {@code task} here refer to the units of physical and logical parallelism, not the programming API.
 */
@InterfaceStability.Evolving
public interface ApplicationDescriptor<S extends ApplicationDescriptor> {

  /**
   * Get the configuration for the application.
   * @return config for the application
   */
  Config getConfig();

  /**
   * Sets the {@link SystemDescriptor} for the default system for the application.
   * <p>
   * The default system is used by the framework for creating any internal (e.g., coordinator, changelog, checkpoint)
   * streams. In an {@link org.apache.samza.application.StreamApplication}, it is also used for creating any
   * intermediate streams; e.g., those created by the {@link org.apache.samza.operators.MessageStream#partitionBy} and
   * {@link org.apache.samza.operators.MessageStream#broadcast} operators.
   * <p>
   * If the default system descriptor is set, it must be set <b>before</b> creating any input/output/intermediate streams.
   *
   * @param defaultSystemDescriptor the {@link SystemDescriptor} for the default system for the application
   * @return this {@link ApplicationDescriptor}
   */
  S withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor);

  /**
   * Sets the {@link ApplicationContainerContextFactory} for this application. Each task will be given access to a
   * different instance of the {@link org.apache.samza.context.ApplicationContainerContext} that this creates. The
   * context can be accessed through the {@link org.apache.samza.context.Context}.
   * <p>
   * Setting this is optional.
   * <p>
   * The provided {@code factory} instance must be {@link java.io.Serializable}.
   *
   * @param factory the {@link ApplicationContainerContextFactory} for this application
   * @return this {@link ApplicationDescriptor}
   */
  S withApplicationContainerContextFactory(ApplicationContainerContextFactory<?> factory);

  /**
   * Sets the {@link ApplicationTaskContextFactory} for this application. Each task will be given access to a different
   * instance of the {@link org.apache.samza.context.ApplicationTaskContext} that this creates. The context can be
   * accessed through the {@link org.apache.samza.context.Context}.
   * <p>
   * Setting this is optional.
   * <p>
   * The provided {@code factory} instance must be {@link java.io.Serializable}.
   *
   * @param factory the {@link ApplicationTaskContextFactory} for this application
   * @return this {@link ApplicationDescriptor}
   */
  S withApplicationTaskContextFactory(ApplicationTaskContextFactory<?> factory);

  /**
   * Sets the {@link ProcessorLifecycleListenerFactory} for this application.
   * <p>
   * Setting a {@link ProcessorLifecycleListenerFactory} is optional to a user application. It allows users to
   * plug in optional code to be invoked in different stages before/after the main processing logic is started/stopped in
   * the application.
   * <p>
   * The provided {@code factory} instance must be {@link java.io.Serializable}.
   *
   * @param listenerFactory the user implemented {@link ProcessorLifecycleListenerFactory} that creates lifecycle listener
   *                        with callback methods before and after the start/stop of each StreamProcessor in the application
   * @return this {@link ApplicationDescriptor}
   */
  S withProcessorLifecycleListenerFactory(ProcessorLifecycleListenerFactory listenerFactory);

  /**
   * Sets the {@link org.apache.samza.metrics.MetricsReporterFactory}s for creating the
   * {@link org.apache.samza.metrics.MetricsReporter}s to use for the application.
   * <p>
   * The provided {@link MetricsReporterFactory} instances must be {@link java.io.Serializable}.
   *
   * @param reporterFactories a map of {@link org.apache.samza.metrics.MetricsReporter} names to their factories.
   * @return this {@link ApplicationDescriptor}
   */
  S withMetricsReporterFactories(Map<String, MetricsReporterFactory> reporterFactories);

}
