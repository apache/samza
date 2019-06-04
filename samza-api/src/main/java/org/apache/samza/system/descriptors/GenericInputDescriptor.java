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

import org.apache.samza.serializers.Serde;

/**
 * A {@link GenericInputDescriptor} can be used for specifying Samza and system-specific properties of
 * input streams.
 * <p>
 * If the system provides its own system and stream descriptor implementations, use them instead.
 * Otherwise, use this {@link GenericInputDescriptor} to specify Samza-specific properties of the stream,
 * and {@link #withStreamConfigs} to specify additional system specific properties.
 * <p>
 * Use {@link GenericSystemDescriptor#getInputDescriptor} to obtain an instance of this descriptor.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptor.
 *
 * @param <StreamMessageType> type of messages in this stream.
 */
public final class GenericInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, GenericInputDescriptor<StreamMessageType>> {
  GenericInputDescriptor(String streamId, SystemDescriptor systemDescriptor, Serde serde) {
    super(streamId, serde, systemDescriptor, null);
  }
}
