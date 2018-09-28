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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ApplicationTaskContext;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
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
public abstract class ApplicationDescriptorImpl<S extends ApplicationDescriptor>
    implements ApplicationDescriptor<S> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationDescriptorImpl.class);

  private final Class<? extends SamzaApplication> appClass;
  private final Map<String, MetricsReporterFactory> reporterFactories = new LinkedHashMap<>();
  // serdes used by input/output/intermediate streams, keyed by streamId
  private final Map<String, KV<Serde, Serde>> streamSerdes = new HashMap<>();
  // serdes used by tables, keyed by tableId
  private final Map<String, KV<Serde, Serde>> tableSerdes = new HashMap<>();
  final Config config;

  /**
   * Optional factory, so value might be null.
   */
  private ApplicationContainerContextFactory<?> applicationContainerContextFactory;

  /**
   * Optional factory, so value might be null.
   */
  private ApplicationTaskContextFactory<?> applicationTaskContextFactory;

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
  public S withApplicationContainerContextFactory(ApplicationContainerContextFactory<?> factory) {
    this.applicationContainerContextFactory = factory;
    return (S) this;
  }

  @Override
  public S withApplicationTaskContextFactory(ApplicationTaskContextFactory<?> factory) {
    this.applicationTaskContextFactory = factory;
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
   * @return {@link ApplicationContainerContextFactory} if application specified it; empty otherwise
   */
  public Optional<ApplicationContainerContextFactory<ApplicationContainerContext>> getApplicationContainerContextFactory() {
    //noinspection unchecked; ok because all context types are at least ApplicationContainerContext
    return Optional.ofNullable(
        (ApplicationContainerContextFactory<ApplicationContainerContext>) this.applicationContainerContextFactory);
  }

  /**
   * @return {@link ApplicationTaskContextFactory} if application specified it; empty otherwise
   */
  public Optional<ApplicationTaskContextFactory<ApplicationTaskContext>> getApplicationTaskContextFactory() {
    //noinspection unchecked; ok because all context types are at least ApplicationTaskContext
    return Optional.ofNullable(
        (ApplicationTaskContextFactory<ApplicationTaskContext>) this.applicationTaskContextFactory);
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

  /**
   * Get the map of all {@link InputOperatorSpec}s in this applicaiton
   *
   * @return an immutable map from streamId to {@link InputOperatorSpec}. Default to empty map for low-level {@link TaskApplication}
   */
  public Map<String, InputOperatorSpec> getInputOperators() {
    return Collections.EMPTY_MAP;
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

  /**
   * Get all the unique input streamIds in this application
   *
   * @return an immutable set of input streamIds
   */
  public abstract Set<String> getInputStreamIds();

  /**
   * Get all the unique output streamIds in this application
   *
   * @return an immutable set of output streamIds
   */
  public abstract Set<String> getOutputStreamIds();

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
    } else if (!currentSerdePair.getKey().equals(keySerde) || !currentSerdePair.getValue().equals(valueSerde)) {
      throw new IllegalArgumentException(String.format("Serde for stream %s is already defined. Cannot change it to "
          + "different serdes.", streamId));
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

}