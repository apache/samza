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
package org.apache.samza.operators.descriptors;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.descriptors.base.system.SimpleSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;

/**
 * A descriptor for a generic system.
 * <p>
 * If the system being used provides its own system and stream descriptor implementations, they should be used instead.
 * Otherwise, this {@link GenericSystemDescriptor} may be used to provide Samza-specific properties of the system.
 * Additional system specific properties may be provided using {@link #withSystemConfigs}
 * <p>
 * System properties configured using a descriptor override corresponding properties provided in configuration.
 *
 * @param <SystemMessageType> type of messages in this system
 */
@SuppressWarnings("unchecked")
public final class GenericSystemDescriptor<SystemMessageType>
    extends SimpleSystemDescriptor<SystemMessageType, GenericSystemDescriptor<SystemMessageType>> {

  /**
   * Constructs a {@link GenericSystemDescriptor} instance with no system level serde.
   * Serdes must be provided explicitly at stream level when getting input or output descriptors.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   */
  public GenericSystemDescriptor(String systemName, String factoryClassName) {
    super(systemName, factoryClassName, null);
  }

  /**
   * Constructs a {@link GenericSystemDescriptor} instance with {@code systemSerde} as the system level serde.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   * @param systemSerde default serde for the system, or null.
   *                    If null, input/output descriptor serde must be provided at a stream level.
   *                    A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be provided if the
   *                    System's consumer deserializes the incoming messages itself, and no further deserialization
   *                    is required from the framework.
   */
  public GenericSystemDescriptor(String systemName, String factoryClassName, Serde<SystemMessageType> systemSerde) {
    super(systemName, factoryClassName, systemSerde);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GenericInputDescriptor<SystemMessageType> getInputDescriptor(String streamId) {
    if (!getSystemSerde().isPresent()) {
      throw new SamzaException(
          String.format("System: %s does not have a system level serde. " +
              "Serde for input stream: %s must be specified explicitly, e.g., using " +
              "GenericSystemDescriptor#getInputDescriptor(String, Serde)", getSystemName(), streamId));
    }
    return new GenericInputDescriptor<>(streamId, this, null, getSystemSerde().get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> GenericInputDescriptor<StreamMessageType> getInputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new GenericInputDescriptor<>(streamId, this, null, serde);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> GenericInputDescriptor<StreamMessageType> getInputDescriptor(
      String streamId, InputTransformer<StreamMessageType> transformer) {
    if (!getSystemSerde().isPresent()) {
      throw new SamzaException(
          String.format("System: %s does not have a system level serde. " +
              "Serde for input stream: %s must be specified explicitly, e.g., using " +
              "GenericSystemDescriptor#getInputDescriptor(String, InputTransformer, Serde)", getSystemName(), streamId));
    }
    return new GenericInputDescriptor<>(streamId, this, transformer, getSystemSerde().get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> GenericInputDescriptor<StreamMessageType> getInputDescriptor(
      String streamId, InputTransformer<StreamMessageType> transformer, Serde serde) {
    return new GenericInputDescriptor<>(streamId, this, transformer, serde);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GenericOutputDescriptor<SystemMessageType> getOutputDescriptor(String streamId) {
    if (!getSystemSerde().isPresent()) {
      throw new SamzaException(
          String.format("System: %s does not have a system level serde. " +
              "Serde for output stream: %s must be specified explicitly, e.g., using " +
              "GenericSystemDescriptor#getOutputDescriptor(String, Serde)", getSystemName(), streamId));
    }
    return new GenericOutputDescriptor<>(streamId, this, getSystemSerde().get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> GenericOutputDescriptor<StreamMessageType> getOutputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new GenericOutputDescriptor<>(streamId, this, serde);
  }
}
