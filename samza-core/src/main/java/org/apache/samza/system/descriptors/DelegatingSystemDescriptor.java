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
package org.apache.samza.system.descriptors;


import com.google.common.annotations.VisibleForTesting;

import org.apache.samza.serializers.Serde;

/**
 * A descriptor for samza framework internal usage.
 * <p>
 * Allows creating a {@link SystemDescriptor} without setting the factory class name, and delegating
 * rest of the system customization to configurations.
 * <p>
 * Useful for code-generation and testing use cases where the factory name is not known in advance.
 */
@SuppressWarnings("unchecked")
public final class DelegatingSystemDescriptor extends SystemDescriptor<DelegatingSystemDescriptor>
    implements SimpleInputDescriptorProvider, OutputDescriptorProvider {

  /**
   * Constructs an {@link DelegatingSystemDescriptor} instance with no system level serde.
   * Serdes must be provided explicitly at stream level when getting input or output descriptors.
   * SystemFactory class name must be provided in configuration.
   *
   * @param systemName name of this system
   */
  @VisibleForTesting
  public DelegatingSystemDescriptor(String systemName) {
    super(systemName, null, null, null);
  }

  @Override
  public <StreamMessageType> GenericInputDescriptor<StreamMessageType> getInputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new GenericInputDescriptor<>(streamId, this, serde);
  }

  @Override
  public <StreamMessageType> GenericOutputDescriptor<StreamMessageType> getOutputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new GenericOutputDescriptor<>(streamId, this, serde);
  }
}
