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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.application.SamzaApplication;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ApplicationTaskContext;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.table.descriptors.HybridTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the base class that implements interface {@link ApplicationDescriptor}.
 * <p>
 * This base class contains the common objects that are used by both high-level and low-level API applications, such as
 * {@link Config}, {@link ApplicationContainerContextFactory}, {@link ApplicationTaskContextFactory}, and
 * {@link ProcessorLifecycleListenerFactory}.
 *
 * @param <S> the type of {@link ApplicationDescriptor} interface this implements. It has to be either
 *            {@link StreamApplicationDescriptor} or {@link TaskApplicationDescriptor}
 */
public abstract class ApplicationDescriptorImpl<S extends ApplicationDescriptor> implements ApplicationDescriptor<S> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationDescriptorImpl.class);
  static final Pattern ID_PATTERN = Pattern.compile("[\\d\\w-_]+");

  private final Class<? extends SamzaApplication> appClass;
  private final Config config;

  // We use a LHMs for deterministic order in initializing and closing operators.
  private final Map<String, InputDescriptor> inputDescriptors = new LinkedHashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new LinkedHashMap<>();
  private final Map<String, SystemDescriptor> systemDescriptors = new LinkedHashMap<>();
  private final Map<String, TableDescriptor> tableDescriptors = new LinkedHashMap<>();
  private Optional<SystemDescriptor> defaultSystemDescriptorOptional = Optional.empty();

  private final Map<String, MetricsReporterFactory> reporterFactories = new LinkedHashMap<>();
  // serdes used by input/output/intermediate streams, keyed by streamId
  private final Map<String, KV<Serde, Serde>> streamSerdes = new HashMap<>();
  // serdes used by tables, keyed by tableId
  private final Map<String, KV<Serde, Serde>> tableSerdes = new HashMap<>();

  private Optional<ApplicationContainerContextFactory<?>> applicationContainerContextFactoryOptional = Optional.empty();
  private Optional<ApplicationTaskContextFactory<?>> applicationTaskContextFactoryOptional = Optional.empty();

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
  public S withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor) {
    Preconditions.checkNotNull(defaultSystemDescriptor, "Provided defaultSystemDescriptor must not be null.");
    Preconditions.checkState(getInputStreamIds().isEmpty() && getOutputStreamIds().isEmpty(),
        "Default system must be set before creating any input or output streams.");
    addSystemDescriptor(defaultSystemDescriptor);

    defaultSystemDescriptorOptional = Optional.of(defaultSystemDescriptor);
    return (S) this;
  }

  @Override
  public S withApplicationContainerContextFactory(ApplicationContainerContextFactory<?> factory) {
    this.applicationContainerContextFactoryOptional = Optional.of(factory);
    return (S) this;
  }

  @Override
  public S withApplicationTaskContextFactory(ApplicationTaskContextFactory<?> factory) {
    this.applicationTaskContextFactoryOptional = Optional.of(factory);
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
   * Get the {@link ApplicationContainerContextFactory} specified by the application.
   *
   * @return {@link ApplicationContainerContextFactory} if application specified it; empty otherwise
   */
  public Optional<ApplicationContainerContextFactory<ApplicationContainerContext>> getApplicationContainerContextFactory() {
    @SuppressWarnings("unchecked") // ok because all context types are at least ApplicationContainerContext
    Optional<ApplicationContainerContextFactory<ApplicationContainerContext>> factoryOptional =
        (Optional) this.applicationContainerContextFactoryOptional;
    return factoryOptional;
  }

  /**
   * Get the {@link ApplicationTaskContextFactory} specified by the application.
   *
   * @return {@link ApplicationTaskContextFactory} if application specified it; empty otherwise
   */
  public Optional<ApplicationTaskContextFactory<ApplicationTaskContext>> getApplicationTaskContextFactory() {
    @SuppressWarnings("unchecked") // ok because all context types are at least ApplicationTaskContext
    Optional<ApplicationTaskContextFactory<ApplicationTaskContext>> factoryOptional =
        (Optional) this.applicationTaskContextFactoryOptional;
    return factoryOptional;
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
   * Get all the unique input streamIds in this application, including any intermediate streams.
   *
   * @return an immutable set of input streamIds
   */
  public Set<String> getInputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(inputDescriptors.keySet()));
  }

  /**
   * Get all the unique output streamIds in this application, including any intermediate streams.
   *
   * @return an immutable set of output streamIds
   */
  public Set<String> getOutputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(outputDescriptors.keySet()));
  }

  /**
   * Get all the intermediate broadcast streamIds for this application
   *
   * @return an immutable set of streamIds
   */
  public Set<String> getIntermediateBroadcastStreamIds() {
    return Collections.emptySet();
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
   * Get all the {@link OutputDescriptor}s for this application
   *
   * @return an immutable map of streamId to {@link OutputDescriptor}
   */
  public Map<String, OutputDescriptor> getOutputDescriptors() {
    return Collections.unmodifiableMap(outputDescriptors);
  }

  /**
   * Get all the {@link SystemDescriptor}s in this application
   *
   * @return an immutable set of {@link SystemDescriptor}s
   */
  public Set<SystemDescriptor> getSystemDescriptors() {
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
   * Get all the {@link TableDescriptor}s in this application
   *
   * @return an immutable set of {@link TableDescriptor}s
   */
  public Set<TableDescriptor> getTableDescriptors() {
    return Collections.unmodifiableSet(new HashSet<>(tableDescriptors.values()));
  }

  /**
   * Get a map of all {@link InputOperatorSpec}s in this application
   *
   * @return an immutable map from streamId to {@link InputOperatorSpec}. Default to empty map for low-level
   * {@link org.apache.samza.application.TaskApplication}
   */
  public Map<String, InputOperatorSpec> getInputOperators() {
    return Collections.emptyMap();
  }

  /**
   * Get the corresponding {@link KVSerde} for the input {@code inputStreamId}
   *
   * @param streamId id of the stream
   * @return the {@link KVSerde} for the stream. null if the serde is not defined or {@code streamId} does not exist
   */
  public KV<Serde, Serde> getStreamSerdes(String streamId) {
    return streamSerdes.get(streamId);
  }

  /**
   * Get the corresponding {@link KVSerde} for the input {@code inputStreamId}
   *
   * @param tableId id of the table
   * @return the {@link KVSerde} for the stream. null if the serde is not defined or {@code streamId} does not exist
   */
  public KV<Serde, Serde> getTableSerdes(String tableId) {
    return tableSerdes.get(tableId);
  }

  KV<Serde, Serde> getOrCreateStreamSerdes(String streamId, Serde serde) {
    Serde keySerde, valueSerde;

    KV<Serde, Serde> currentSerdePair = streamSerdes.get(streamId);

    if (serde instanceof KVSerde) {
      keySerde = ((KVSerde) serde).getKeySerde();
      valueSerde = ((KVSerde) serde).getValueSerde();
    } else {
      keySerde = new NoOpSerde();
      valueSerde = serde;
    }

    if (currentSerdePair == null) {
      if (keySerde instanceof NoOpSerde) {
        LOGGER.info("Using NoOpSerde as the key serde for stream " + streamId +
            ". Keys will not be (de)serialized");
      }
      if (valueSerde instanceof NoOpSerde) {
        LOGGER.info("Using NoOpSerde as the value serde for stream " + streamId +
            ". Values will not be (de)serialized");
      }
      streamSerdes.put(streamId, KV.of(keySerde, valueSerde));
    } else if (!currentSerdePair.getKey().getClass().equals(keySerde.getClass())
        || !currentSerdePair.getValue().getClass().equals(valueSerde.getClass())) {
      throw new IllegalArgumentException(String.format("Serde for streamId: %s is already defined. Cannot change it to "
          + "different serdes.", streamId));
    } else {
      LOGGER.warn("Using previously defined serde for streamId: " + streamId + ".");
    }
    return streamSerdes.get(streamId);
  }

  KV<Serde, Serde> getOrCreateTableSerdes(String tableId, KVSerde kvSerde) {
    Serde keySerde, valueSerde;
    keySerde = kvSerde.getKeySerde();
    valueSerde = kvSerde.getValueSerde();

    if (!tableSerdes.containsKey(tableId)) {
      tableSerdes.put(tableId, KV.of(keySerde, valueSerde));
      return tableSerdes.get(tableId);
    }

    KV<Serde, Serde> currentSerdePair = tableSerdes.get(tableId);
    if (!currentSerdePair.getKey().equals(keySerde) || !currentSerdePair.getValue().equals(valueSerde)) {
      throw new IllegalArgumentException(String.format("Serde for table %s is already defined. Cannot change it to "
          + "different serdes.", tableId));
    }
    return streamSerdes.get(tableId);
  }

  final void addInputDescriptor(InputDescriptor inputDescriptor) {
    String streamId = inputDescriptor.getStreamId();
    Preconditions.checkState(!inputDescriptors.containsKey(streamId)
            || inputDescriptors.get(streamId) == inputDescriptor,
        String.format("Cannot add multiple input descriptors with the same streamId: %s", streamId));
    inputDescriptors.put(streamId, inputDescriptor);
    addSystemDescriptor(inputDescriptor.getSystemDescriptor());
  }

  final void addOutputDescriptor(OutputDescriptor outputDescriptor) {
    String streamId = outputDescriptor.getStreamId();
    Preconditions.checkState(!outputDescriptors.containsKey(streamId)
            || outputDescriptors.get(streamId) == outputDescriptor,
        String.format("Cannot add an output descriptor multiple times with the same streamId: %s", streamId));
    outputDescriptors.put(streamId, outputDescriptor);
    addSystemDescriptor(outputDescriptor.getSystemDescriptor());
  }

  final void addTableDescriptor(TableDescriptor tableDescriptor) {
    String tableId = tableDescriptor.getTableId();
    Preconditions.checkState(StringUtils.isNotBlank(tableId) && ID_PATTERN.matcher(tableId).matches(),
        String.format("tableId: %s must confirm to pattern: %s", tableId, ID_PATTERN.toString()));
    Preconditions.checkState(!tableDescriptors.containsKey(tableId)
        || tableDescriptors.get(tableId) == tableDescriptor,
        String.format("Cannot add multiple table descriptors with the same tableId: %s", tableId));

    if (tableDescriptor instanceof HybridTableDescriptor) {
      List<? extends TableDescriptor> tableDescs =
          ((HybridTableDescriptor) tableDescriptor).getTableDescriptors();
      tableDescs.forEach(td -> addTableDescriptor(td));
    }

    tableDescriptors.put(tableId, tableDescriptor);
  }

  // check uniqueness of the {@code systemDescriptor} and add if it is unique
  private void addSystemDescriptor(SystemDescriptor systemDescriptor) {
    String systemName = systemDescriptor.getSystemName();
    Preconditions.checkState(!systemDescriptors.containsKey(systemName)
            || systemDescriptors.get(systemName) == systemDescriptor,
        "Must not use different system descriptor instances for the same system name: " + systemName);
    systemDescriptors.put(systemName, systemDescriptor);
  }
}