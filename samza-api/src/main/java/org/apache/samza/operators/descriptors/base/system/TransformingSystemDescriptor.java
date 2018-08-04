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

import org.apache.samza.SamzaException;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;

/**
 * The base descriptor for a system that provides a default {@link InputTransformer} implementation for every
 * input stream, in addition to the system factory class name and optionally a default system level serde.
 *
 * @param <SystemMessageType> default type of messages in this system.
 * @param <SystemTransformerType> default type of the {@link InputTransformer} results
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class TransformingSystemDescriptor<SystemMessageType, SystemTransformerType, SubClass extends TransformingSystemDescriptor<SystemMessageType, SystemTransformerType, SubClass>>
    extends SystemDescriptor<SystemMessageType, SubClass> {
  private final InputTransformer<SystemTransformerType> transformer;

  /**
   * Constructs a {@link TransformingSystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   * @param systemSerde default serde for the system, or null.
   *                    If null, input/output descriptor serde must be provided at a stream level.
   *                    A {@code KVSerde<NoOpSerde, NoOpSerde>} or {@code NoOpSerde} may be provided if the
   *                    System's consumer deserializes the incoming messages itself, and no further deserialization
   *                    is required from the framework.
   * @param transformer default input stream transformer for this system
   */
  public TransformingSystemDescriptor(String systemName, String factoryClassName, Serde<SystemMessageType> systemSerde,
      InputTransformer<SystemTransformerType> transformer) {
    super(systemName, factoryClassName, systemSerde);
    if (transformer == null) {
      throw new SamzaException("Default InputTransformer may not be null for a TransformingSystemDescriptor");
    }
    this.transformer = transformer;
  }

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the default
   * system level serde, and the default system level level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the default system level
   * {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract InputDescriptor<SystemTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId);

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the default system level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the provided stream level
   * {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @param serde stream level serde for the input stream
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract InputDescriptor<SystemTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId, Serde serde);

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the default
   * system level serde, and the provided stream level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the default system level
   * {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @param transformer stream level {@link InputTransformer} for the input stream
   * @param <StreamTransformerType> type of messages returned by the {@code transformer}
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract <StreamTransformerType> InputDescriptor<StreamTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId, InputTransformer<StreamTransformerType> transformer);

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the provided stream level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the provided stream level
   * {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @param transformer stream level {@link InputTransformer} for the input stream
   * @param serde stream level serde for the input stream
   * @param <StreamTransformerType> type of messages returned by the {@code transformer}
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract <StreamTransformerType> InputDescriptor<StreamTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId, InputTransformer<StreamTransformerType> transformer, Serde serde);

  public InputTransformer<SystemTransformerType> getTransformer() {
    return this.transformer;
  }
}
