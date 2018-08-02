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
package org.apache.samza.operators.descriptors.base.stream;

import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;

/**
 * The base descriptor for an output stream. Allows setting properties that are common to all output streams.
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
   * @param systemName system name for the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from if available, else null
   */
  public OutputDescriptor(String streamId, String systemName, Serde<StreamMessageType> serde,
      SystemDescriptor systemDescriptor) {
    super(streamId, systemName, serde, systemDescriptor);
  }
}
