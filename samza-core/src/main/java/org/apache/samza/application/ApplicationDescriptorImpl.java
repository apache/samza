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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
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
  final Class<? extends SamzaApplication> appClass;

  private final Map<String, InputDescriptor> inputDescriptors = new LinkedHashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new LinkedHashMap<>();
  private final Map<String, SystemDescriptor> systemDescriptors = new LinkedHashMap<>();
  private final Set<String> broadcastStreams = new HashSet<>();
  private final Map<String, TableDescriptor> tableDescriptors = new LinkedHashMap<>();
  private final Map<String, MetricsReporterFactory> reporterFactories = new LinkedHashMap<>();

  private Optional<SystemDescriptor> defaultSystemDescriptorOptional = Optional.empty();

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

  ApplicationDescriptorImpl(SamzaApplication userApp, Config config) {
    this.config = config;
    this.appClass = userApp.getClass();
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
  public S withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor) {
    Preconditions.checkNotNull(defaultSystemDescriptor, "Provided defaultSystemDescriptor must not be null.");
    Preconditions.checkState(noInputOutputStreams(),
        "Default system must be set before creating any input or output streams.");
    addSystemDescriptor(defaultSystemDescriptor);
    defaultSystemDescriptorOptional = Optional.of(defaultSystemDescriptor);
    return (S) this;
  }

  @Override
  public S withMetricsReporterFactories(Map<String, MetricsReporterFactory> reporterFactories) {
    this.reporterFactories.clear();
    this.reporterFactories.putAll(reporterFactories);
    return (S) this;
  }
  /**
   * Get the user application class
   *
   * @return user implemented {@link SamzaApplication} class
   */
  public Class<? extends SamzaApplication> getAppClass() {
    return appClass;
  }

  /**
   * Get the user-implemented {@link ContextManager} object associated with this application
   *
   * @return the {@link ContextManager} object
   */
  public ContextManager getContextManager() {
    return contextManager;
  }

  /**
   * Get the user-implemented {@link ProcessorLifecycleListenerFactory} object associated with this application
   *
   * @return the {@link ProcessorLifecycleListenerFactory} object
   */
  public ProcessorLifecycleListenerFactory getProcessorLifecycleListenerFactory() {
    return listenerFactory;
  }

  /**
   * Get all the {@link InputDescriptor}s to this application
   *
   * @return an immutable map of streamId to {@link InputDescriptor}
   */
  public Map<String, InputDescriptor> getInputDescriptors() {
    return Collections.unmodifiableMap(inputDescriptors);
  }

  /**
   * Get all the {@link OutputDescriptor}s from this application
   *
   * @return an immutable map of streamId to {@link OutputDescriptor}
   */
  public Map<String, OutputDescriptor> getOutputDescriptors() {
    return Collections.unmodifiableMap(outputDescriptors);
  }

  /**
   * Get all the broadcast streamIds from this application
   *
   * @return an immutable set of streamIds
   */
  public Set<String> getBroadcastStreams() {
    return Collections.unmodifiableSet(broadcastStreams);
  }

  /**
   * Get all the {@link TableDescriptor}s in this application
   *
   * @return an immutable set of {@link TableDescriptor}s
   */
  public Set<TableDescriptor> getTableDescriptors() {
    return Collections.unmodifiableSet(new HashSet<>(tableDescriptors.values()));
  }

  /**
   * Get all the unique {@link SystemDescriptor}s in this application
   *
   * @return an immutable set of {@link SystemDescriptor}s
   */
  public Set<SystemDescriptor> getSystemDescriptors() {
    // We enforce that users must not use different system descriptor instances for the same system name
    // when getting an input/output stream or setting the default system descriptor
    return Collections.unmodifiableSet(new HashSet<>(systemDescriptors.values()));
  }

  /**
   * Get the default {@link SystemDescriptor} in this application
   *
   * @return the default {@link SystemDescriptor}
   */
  public Optional<SystemDescriptor> getDefaultSystemDescriptor() {
    return defaultSystemDescriptorOptional;
  }

  /**
   * Get the {@link MetricsReporterFactory}s used in the application
   *
   * @return the map of {@link MetricsReporterFactory}s
   */
  public Map<String, MetricsReporterFactory> getMetricsReporterFactories() {
    return Collections.unmodifiableMap(reporterFactories);
  }

  // TODO: this should go away when partitionBy() and broadcast() will also generate InputDescriptor/OutputDescriptor as well
  // helper method to determine that there is no input/output streams added in the application yet
  protected abstract boolean noInputOutputStreams();

  // internal method to add {@link TableDescriptor} to this application
  void addTableDescriptor(TableDescriptor tableDescriptor) {
    Preconditions.checkState(!tableDescriptors.containsKey(tableDescriptor.getTableId()),
        String.format("add table descriptors multiple times with the same tableId: %s", tableDescriptor.getTableId()));
    tableDescriptors.put(tableDescriptor.getTableId(), tableDescriptor);
  }

  // internal method to add {@link InputDescriptor} to this application
  void addInputDescriptor(InputDescriptor isd) {
    // TODO: need to add to the broadcast streams if isd is a broadcast stream
    Preconditions.checkState(!inputDescriptors.containsKey(isd.getStreamId()),
        String.format("add input descriptors multiple times with the same streamId: %s", isd.getStreamId()));
    inputDescriptors.put(isd.getStreamId(), isd);
    addSystemDescriptor(isd.getSystemDescriptor());
  }

  // internal method to add {@link OutputDescriptor} to this application
  void addOutputDescriptor(OutputDescriptor osd) {
    // TODO: need to add to the broadcast streams if osd is a broadcast stream
    Preconditions.checkState(!outputDescriptors.containsKey(osd.getStreamId()),
        String.format("add output descriptors multiple times with the same streamId: %s", osd.getStreamId()));
    outputDescriptors.put(osd.getStreamId(), osd);
    addSystemDescriptor(osd.getSystemDescriptor());
  }

  // TODO: this should be completely internal to addInputDescriptor()/addOutputDescriptor after we add broadcast automatically
  void addBroadcastStream(String streamId) {
    broadcastStreams.add(streamId);
  }

  // internal method to add a unique {@link SystemDescriptor} to this application
  private void addSystemDescriptor(SystemDescriptor systemDescriptor) {
    Preconditions.checkState(!systemDescriptors.containsKey(systemDescriptor.getSystemName())
            || systemDescriptors.get(systemDescriptor.getSystemName()) == systemDescriptor,
        "Must not use different system descriptor instances for the same system name: " + systemDescriptor.getSystemName());
    systemDescriptors.put(systemDescriptor.getSystemName(), systemDescriptor);
  }
}