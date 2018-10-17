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
 * The interface class to describe the configuration, input and output streams, and processing logic in a
 * {@link org.apache.samza.application.SamzaApplication}.
 * <p>
 * Sub-classes {@link StreamApplicationDescriptor} and {@link TaskApplicationDescriptor} are specific interfaces for
 * applications written in high-level {@link org.apache.samza.application.StreamApplication} and low-level
 * {@link org.apache.samza.application.TaskApplication} APIs, respectively.
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
   * Sets the default SystemDescriptor to use for the application. This is equivalent to setting
   * {@code job.default.system} and its properties in configuration.
   * <p>
   * If the default system descriptor is set, it must be set <b>before</b> creating any input/output/intermediate streams.
   *
   * @param defaultSystemDescriptor the default system descriptor to use
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code defaultSystemDescriptor} set as its default system
   */
  S withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor);

  /**
   * Sets the {@link ApplicationContainerContextFactory} for this application. Each task will be given access to a
   * different instance of the {@link org.apache.samza.context.ApplicationContainerContext} that this creates. The
   * context can be accessed through the {@link org.apache.samza.context.Context}.
   * <p>
   * Setting this is optional.
   *
   * @param factory the {@link ApplicationContainerContextFactory} for this application
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code factory} set as its
   * {@link ApplicationContainerContextFactory}
   */
  S withApplicationContainerContextFactory(ApplicationContainerContextFactory<?> factory);

  /**
   * Sets the {@link ApplicationTaskContextFactory} for this application. Each task will be given access to a different
   * instance of the {@link org.apache.samza.context.ApplicationTaskContext} that this creates. The context can be
   * accessed through the {@link org.apache.samza.context.Context}.
   * <p>
   * Setting this is optional.
   *
   * @param factory the {@link ApplicationTaskContextFactory} for this application
   * @return type {@code S} of {@link ApplicationDescriptor} with {@code factory} set as its
   * {@link ApplicationTaskContextFactory}
   */
  S withApplicationTaskContextFactory(ApplicationTaskContextFactory<?> factory);

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
