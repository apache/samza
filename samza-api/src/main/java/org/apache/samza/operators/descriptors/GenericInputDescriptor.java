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

import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;

/**
 * A descriptor for a generic input stream.
 * <p>
 * An instance of this descriptor may be obtained from an appropriately configured {@link GenericSystemDescriptor}.
 * <p>
 * If the system being used provides its own system and stream descriptor implementations, they should be used instead.
 * Otherwise, this {@link GenericInputDescriptor} may be used to provide Samza-specific properties of the input stream.
 * Additional system stream specific properties may be provided using {@link #withStreamConfigs}
 * <p>
 * Stream properties configured using a descriptor override corresponding properties provided in configuration.
 *
 * @param <StreamMessageType> type of messages in this stream.
 */
public final class GenericInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, GenericInputDescriptor<StreamMessageType>> {
  GenericInputDescriptor(String streamId, SystemDescriptor systemDescriptor,
      InputTransformer<StreamMessageType> transformer, Serde serde) {
    super(streamId, systemDescriptor.getSystemName(), serde, systemDescriptor, transformer);
  }
}
