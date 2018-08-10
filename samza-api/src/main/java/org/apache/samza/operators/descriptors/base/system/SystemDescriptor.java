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
package org.apache.samza.operators.descriptors.base.system;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamMetadata.OffsetType;

/**
 * The base descriptor for a system. Allows setting properties that are common to all systems.
 *
 * @param <SubClass> type of the concrete sub-class
 */
@SuppressWarnings("unchecked")
public abstract class SystemDescriptor<SubClass extends SystemDescriptor<SubClass>> {
  private static final String FACTORY_CONFIG_KEY = "systems.%s.samza.factory";
  private static final String DEFAULT_STREAM_OFFSET_DEFAULT_CONFIG_KEY = "systems.%s.default.stream.samza.offset.default";
  private static final String DEFAULT_STREAM_CONFIGS_CONFIG_KEY = "systems.%s.default.stream.%s";
  private static final String SYSTEM_CONFIGS_CONFIG_KEY = "systems.%s.%s";
  private static final Pattern ID_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  private final String systemName;
  private final String factoryClassName;

  private Optional<OffsetType> defaultStreamOffsetDefaultOptional = Optional.empty();
  private Map<String, String> systemConfigs = new HashMap<>();
  private Map<String, String> defaultStreamConfigs = new HashMap<>();

  /**
   * Constructs a {@link SystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   */
  SystemDescriptor(String systemName, String factoryClassName) {
    Preconditions.checkArgument(isValidId(systemName),
        String.format("systemName: %s must be non-empty and must not contain spaces or special characters.", systemName));
    Preconditions.checkArgument(StringUtils.isNotBlank(factoryClassName),
        String.format("SystemFactory class name for system: %s must not be null or empty: %s.", systemName, factoryClassName));

    this.systemName = systemName;
    this.factoryClassName = factoryClassName;
  }

  /**
   * If a container starts up without a checkpoint, this property determines where in the input stream we should start
   * consuming. The value must be an {@link OffsetType}, one of the following:
   * <ul>
   *  <li>upcoming: Start processing messages that are published after the job starts.
   *                Any messages published while the job was not running are not processed.
   *  <li>oldest: Start processing at the oldest available message in the system,
   *              and reprocess the entire available message history.
   * </ul>
   * This property is for all streams obtained using this system descriptor. To set it for an individual stream,
   * see {@link org.apache.samza.operators.descriptors.base.stream.InputDescriptor#withOffsetDefault}.
   * If both are defined, the stream-level definition takes precedence.
   *
   * @param offsetType offset type to start processing from
   * @return this system descriptor
   */
  public SubClass withDefaultStreamOffsetDefault(OffsetType offsetType) {
    this.defaultStreamOffsetDefaultOptional = Optional.ofNullable(offsetType);
    return (SubClass) this;
  }

  /**
   * Additional system-specific properties for this system.
   * <p>
   * These properties are added under the {@code systems.system-name.*} scope.
   *
   * @param systemConfigs system-specific properties for this system
   * @return this system descriptor
   */
  public SubClass withSystemConfigs(Map<String, String> systemConfigs) {
    this.systemConfigs.putAll(systemConfigs);
    return (SubClass) this;
  }

  /**
   * Default properties for any stream obtained using this system descriptor.
   * <p>
   * For example, if "systems.kafka-system.default.stream.replication.factor"=2 was configured,
   * then every Kafka stream created on the kafka-system will have a replication factor of 2
   * unless the property is explicitly overridden using the stream descriptor.
   *
   * @param defaultStreamConfigs default stream properties
   * @return this system descriptor
   */
  public SubClass withDefaultStreamConfigs(Map<String, String> defaultStreamConfigs) {
    this.defaultStreamConfigs.putAll(defaultStreamConfigs);
    return (SubClass) this;
  }

  public String getSystemName() {
    return this.systemName;
  }

  private String getFactoryClassName() {
    return this.factoryClassName;
  }

  private Optional<OffsetType> getDefaultStreamOffsetDefault() {
    return this.defaultStreamOffsetDefaultOptional;
  }

  private Map<String, String> getSystemConfigs() {
    return Collections.unmodifiableMap(this.systemConfigs);
  }

  private Map<String, String> getDefaultStreamConfigs() {
    return Collections.unmodifiableMap(this.defaultStreamConfigs);
  }

  private boolean isValidId(String id) {
    return StringUtils.isNotBlank(id) && ID_PATTERN.matcher(id).matches();
  }

  /**
   * Get an {@link OutputDescriptor} representing an output stream on this system that uses the provided
   * stream specific serde instead of the default system serde.
   * <p>
   * An {@code OutputStream<KV<K, V>>}, obtained using a descriptor with a {@code KVSerde<K, V>}, can send messages
   * of type {@code KV<K, V>}. An {@code OutputStream<M>} with any other {@code Serde<M>} can send messages of
   * type M without a key.
   * <p>
   * A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be used if the {@code SystemProducer}
   * serializes the outgoing messages itself, and no prior serialization is required from the framework.
   *
   * @param streamId id of the output stream
   * @param serde serde for this output stream that overrides the default system serde, if any.
   * @param <StreamMessageType> type of messages in the output stream
   * @return the {@link OutputDescriptor} for the output stream
   */
  public abstract <StreamMessageType> OutputDescriptor<StreamMessageType, ? extends OutputDescriptor> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde);

  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>();
    configs.put(String.format(FACTORY_CONFIG_KEY, systemName), getFactoryClassName());
    getDefaultStreamOffsetDefault().ifPresent(dsod ->
        configs.put(String.format(DEFAULT_STREAM_OFFSET_DEFAULT_CONFIG_KEY, systemName), dsod.name().toLowerCase()));
    getDefaultStreamConfigs().forEach((key, value) ->
        configs.put(String.format(DEFAULT_STREAM_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    getSystemConfigs().forEach((key, value) ->
        configs.put(String.format(SYSTEM_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    return configs;
  }
}
