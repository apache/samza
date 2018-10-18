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

package org.apache.samza.test.framework.system.descriptors;

import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.serializers.NoOpSerde;

/**
 * A descriptor for an in memory stream of messages that can either have single or multiple partitions.
 * <p>
 * An instance of this descriptor may be obtained from an appropriately configured {@link InMemorySystemDescriptor}.
 *
 * @param <StreamMessageType> type of messages in input stream
 */
public class InMemoryInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, InMemoryInputDescriptor<StreamMessageType>> {
  /**
   * Constructs a new InMemoryInputDescriptor from specified components.
   * @param systemDescriptor name of the system stream is associated with
   * @param streamId name of the stream
   */
  InMemoryInputDescriptor(String streamId, InMemorySystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde<>(), systemDescriptor, null);
  }
}
