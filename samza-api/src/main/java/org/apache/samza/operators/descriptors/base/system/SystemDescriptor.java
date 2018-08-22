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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.functions.StreamExpander;
import org.apache.samza.system.SystemStreamMetadata.OffsetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base descriptor for a system. Allows setting properties that are common to all systems.
 * <p>
 * System properties configured using a descriptor override corresponding properties provided in configuration.
 * <p>
 * Systems may provide an {@link InputTransformer} to be used for input streams on the system. An
 * {@link InputTransformer} transforms an {@code IncomingMessageEnvelope} with deserialized key and message
 * to another message that is delivered to the {@code MessageStream}. It is applied at runtime in
 * {@code InputOperatorImpl}.
 * <p>
 * Systems may provide a {@link StreamExpander} to be used for input streams on the system. A {@link StreamExpander}
 * expands the provided {@code InputDescriptor} to a sub-DAG of one or more operators on the {@code StreamGraph},
 * and returns a new {@code MessageStream} with the combined results. It is called during graph description
 * in {@code StreamGraph#getInputStream}.
 * <p>
 * Systems that support consuming messages from a stream should provide users means of obtaining an
 * {@code InputDescriptor}. Recommended interfaces for doing so are {@link TransformingInputDescriptorProvider} for
 * systems that support system level {@link InputTransformer}, {@link ExpandingInputDescriptorProvider} for systems
 * that support system level {@link StreamExpander} functions, and {@link SimpleInputDescriptorProvider} otherwise.
 * <p>
 * Systems that support producing messages to a stream should provide users means of obtaining an
 * {@code OutputDescriptor}. Recommended interface for doing so is {@link OutputDescriptorProvider}.
 * <p>
 * It is not required for SystemDescriptors to implement one of the Provider interfaces above. System implementers
 * may choose to expose additional or alternate APIs for obtaining Input/Output Descriptors by extending
 * SystemDescriptor directly.
 *
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class SystemDescriptor<SubClass extends SystemDescriptor<SubClass>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemDescriptor.class);
  private static final String FACTORY_CONFIG_KEY = "systems.%s.samza.factory";
  private static final String DEFAULT_STREAM_OFFSET_DEFAULT_CONFIG_KEY = "systems.%s.default.stream.samza.offset.default";
  private static final String DEFAULT_STREAM_CONFIGS_CONFIG_KEY = "systems.%s.default.stream.%s";
  private static final String SYSTEM_CONFIGS_CONFIG_KEY = "systems.%s.%s";
  private static final Pattern ID_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  private final String systemName;
  private final Optional<String> factoryClassNameOptional;
  private final Optional<InputTransformer> transformerOptional;
  private final Optional<StreamExpander> expanderOptional;

  private final Map<String, String> systemConfigs = new HashMap<>();
  private final Map<String, String> defaultStreamConfigs = new HashMap<>();
  private Optional<OffsetType> defaultStreamOffsetDefaultOptional = Optional.empty();

  /**
   * Constructs a {@link SystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   * @param transformer the {@link InputTransformer} for the system if any, else null
   * @param expander the {@link StreamExpander} for the system if any, else null
   */
  public SystemDescriptor(String systemName, String factoryClassName, InputTransformer transformer, StreamExpander expander) {
    Preconditions.checkArgument(isValidId(systemName),
        String.format("systemName: %s must be non-empty and must not contain spaces or special characters.", systemName));
    if (StringUtils.isBlank(factoryClassName)) {
      LOGGER.warn("Blank SystemFactory class name for system: {}. A value must be provided in configuration using {}.",
          systemName, String.format(FACTORY_CONFIG_KEY, systemName));
    }
    this.systemName = systemName;
    this.factoryClassNameOptional = Optional.ofNullable(StringUtils.stripToNull(factoryClassName));
    this.transformerOptional = Optional.ofNullable(transformer);
    this.expanderOptional = Optional.ofNullable(expander);
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

  public Optional<InputTransformer> getTransformer() {
    return this.transformerOptional;
  }

  public Optional<StreamExpander> getExpander() {
    return this.expanderOptional;
  }

  private boolean isValidId(String id) {
    return StringUtils.isNotBlank(id) && ID_PATTERN.matcher(id).matches();
  }

  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>();
    this.factoryClassNameOptional.ifPresent(name -> configs.put(String.format(FACTORY_CONFIG_KEY, systemName), name));
    this.defaultStreamOffsetDefaultOptional.ifPresent(dsod ->
        configs.put(String.format(DEFAULT_STREAM_OFFSET_DEFAULT_CONFIG_KEY, systemName), dsod.name().toLowerCase()));
    this.defaultStreamConfigs.forEach((key, value) ->
        configs.put(String.format(DEFAULT_STREAM_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    this.systemConfigs.forEach((key, value) ->
        configs.put(String.format(SYSTEM_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    return configs;
  }
}
