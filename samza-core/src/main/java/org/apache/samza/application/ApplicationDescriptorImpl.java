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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.task.TaskContext;


/**
 * This is the base class that implements interface {@link ApplicationDescriptor}.
 * <p>
 * This base class contains the common objects that are used by both high-level and low-level API applications, such as
 * {@link Config}, {@link ContextManager}, and {@link ProcessorLifecycleListenerFactory}.
 *
 * @param <S> the type of {@link ApplicationDescriptor} interface this implements. It has to be either
 *            {@link StreamApplicationDescriptor} or {@link TaskApplicationDescriptor}
 */
public abstract class ApplicationDescriptorImpl<S extends ApplicationDescriptor>
    implements ApplicationDescriptor<S> {

  final Config config;
  private final Class<? extends SamzaApplication> appClass;
  private final Map<String, MetricsReporterFactory> reporterFactories = new LinkedHashMap<>();

  // Default to no-op functions in ContextManager
  // TODO: this should be replaced by shared context factory defined in SAMZA-1714
  ContextManager contextManager = new ContextManager() {
    @Override
    public void init(Config config, TaskContext context) {
    }

    @Override
    public void close() {
    }
  };

  // Default to no-op  ProcessorLifecycleListenerFactory
  ProcessorLifecycleListenerFactory listenerFactory = (pcontext, cfg) -> new ProcessorLifecycleListener() { };

  ApplicationDescriptorImpl(SamzaApplication app, Config config) {
    this.config = config;
    this.appClass = app.getClass();
  }

  @Override
  public Config getConfig() {
    return config;
  }

  @Override
  public S withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return (S) this;
  }

  @Override
  public S withProcessorLifecycleListenerFactory(ProcessorLifecycleListenerFactory listenerFactory) {
    this.listenerFactory = listenerFactory;
    return (S) this;
  }

  @Override
  public S withMetricsReporterFactories(Map<String, MetricsReporterFactory> reporterFactories) {
    this.reporterFactories.clear();
    this.reporterFactories.putAll(reporterFactories);
    return (S) this;
  }

  /**
   * Get the application class
   *
   * @return an implementation of {@link SamzaApplication}
   */
  public Class<? extends SamzaApplication> getAppClass() {
    return appClass;
  }

  /**
   * Get the {@link ContextManager} associated with this application
   *
   * @return the {@link ContextManager} for this application
   */
  public ContextManager getContextManager() {
    return contextManager;
  }

  /**
   * Get the {@link ProcessorLifecycleListenerFactory} associated with this application
   *
   * @return the {@link ProcessorLifecycleListenerFactory} in this application
   */
  public ProcessorLifecycleListenerFactory getProcessorLifecycleListenerFactory() {
    return listenerFactory;
  }

  /**
   * Get the {@link MetricsReporterFactory}s used in the application
   *
   * @return the map of {@link MetricsReporterFactory}s
   */
  public Map<String, MetricsReporterFactory> getMetricsReporterFactories() {
    return Collections.unmodifiableMap(reporterFactories);
  }

  /**
   * Get the default {@link SystemDescriptor} in this application
   *
   * @return the default {@link SystemDescriptor}
   */
  public Optional<SystemDescriptor> getDefaultSystemDescriptor() {
    // default is not set
    return Optional.empty();
  }

  /**
   * Get all the {@link InputDescriptor}s to this application
   *
   * @return an immutable map of streamId to {@link InputDescriptor}
   */
  public abstract Map<String, InputDescriptor> getInputDescriptors();

  /**
   * Get all the {@link OutputDescriptor}s from this application
   *
   * @return an immutable map of streamId to {@link OutputDescriptor}
   */
  public abstract Map<String, OutputDescriptor> getOutputDescriptors();

  /**
   * Get all the broadcast streamIds from this application
   *
   * @return an immutable set of streamIds
   */
  public abstract Set<String> getBroadcastStreams();

  /**
   * Get all the {@link TableDescriptor}s in this application
   *
   * @return an immutable set of {@link TableDescriptor}s
   */
  public abstract Set<TableDescriptor> getTableDescriptors();

  /**
   * Get all the unique {@link SystemDescriptor}s in this application
   *
   * @return an immutable set of {@link SystemDescriptor}s
   */
  public abstract Set<SystemDescriptor> getSystemDescriptors();

}