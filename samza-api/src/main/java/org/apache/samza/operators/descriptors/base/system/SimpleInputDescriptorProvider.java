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

import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.serializers.Serde;


/**
 * Interface for simple {@code SystemDescriptors} that return {@code InputDescriptors} parameterized by the type of
 * the provided stream level serde.
 */
public interface SimpleInputDescriptorProvider {

  /**
   * Gets an {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde.
   * <p>
   * The type of messages in the stream is the type of the provided stream level serde.
   *
   * @param streamId id of the input stream
   * @param serde stream level serde for the input stream
   * @param <StreamMessageType> type of messages in this stream
   * @return an {@link InputDescriptor} for the input stream
   */
  <StreamMessageType> InputDescriptor<StreamMessageType, ? extends InputDescriptor> getInputDescriptor(String streamId, Serde<StreamMessageType> serde);
}
