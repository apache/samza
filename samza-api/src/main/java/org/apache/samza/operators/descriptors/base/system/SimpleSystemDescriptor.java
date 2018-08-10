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
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;

/**
 * The base descriptor for a simple system that provides the system factory class name.
 *
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class SimpleSystemDescriptor<SubClass extends SimpleSystemDescriptor<SubClass>> extends SystemDescriptor<SubClass> {

  /**
   * Constructs a {@link SimpleSystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   */
  public SimpleSystemDescriptor(String systemName, String factoryClassName) {
    super(systemName, factoryClassName);
  }

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
  public abstract <StreamMessageType> InputDescriptor<StreamMessageType, ? extends InputDescriptor> getInputDescriptor(String streamId, Serde<StreamMessageType> serde);

  /**
   * Gets an {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the provided stream level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the provided {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @param transformer stream level serde for the input stream
   * @param serde stream level serde for the input stream
   * @param <StreamTransformerType> type of messages in this stream
   * @return an {@link InputDescriptor} for the input stream
   */
  public abstract <StreamTransformerType> InputDescriptor<StreamTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId, InputTransformer<StreamTransformerType> transformer, Serde serde);
}
