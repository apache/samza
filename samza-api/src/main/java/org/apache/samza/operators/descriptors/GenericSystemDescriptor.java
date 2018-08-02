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
 * @param <SystemMessageType> type of messages in this stream.
 */
public final class GenericSystemDescriptor<SystemMessageType>
    extends SimpleSystemDescriptor<SystemMessageType, GenericSystemDescriptor<SystemMessageType>> {
  public GenericSystemDescriptor(String systemName, String factoryClassName, Serde<SystemMessageType> serde) {
    super(systemName, factoryClassName, serde);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GenericInputDescriptor<SystemMessageType> getInputDescriptor(String streamId) {
    return new GenericInputDescriptor<>(streamId, this, null, getSerde());
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
    return new GenericInputDescriptor<>(streamId, this, transformer, getSerde());
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
    return new GenericOutputDescriptor<>(streamId, this, getSerde());
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
