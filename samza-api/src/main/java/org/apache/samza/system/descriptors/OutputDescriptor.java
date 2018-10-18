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
 * An {@link OutputDescriptor} can be used for specifying Samza and system-specific properties of output streams.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptor.
 * <p>
 * This is the base descriptor for an output stream. Use a system-specific input descriptor (e.g. KafkaOutputDescriptor)
 * obtained from its system descriptor (e.g. KafkaSystemDescriptor) if one is available. Otherwise use the
 * {@link GenericOutputDescriptor} obtained from a {@link GenericSystemDescriptor}.
 *
 * @param <StreamMessageType> type of messages in this stream.
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class OutputDescriptor<StreamMessageType, SubClass extends OutputDescriptor<StreamMessageType, SubClass>>
    extends StreamDescriptor<StreamMessageType, SubClass> {
  /**
   * Constructs an {@link OutputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  public OutputDescriptor(String streamId, Serde serde, SystemDescriptor systemDescriptor) {
    super(streamId, serde, systemDescriptor);
  }
}
