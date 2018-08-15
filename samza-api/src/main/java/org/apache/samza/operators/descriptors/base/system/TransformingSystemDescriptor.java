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
 * @param <SystemTransformerType> default type of the {@link InputTransformer} results
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class TransformingSystemDescriptor<SystemTransformerType, SubClass extends TransformingSystemDescriptor<SystemTransformerType, SubClass>>
    extends SystemDescriptor<SubClass> {

  /**
   * Constructs a {@link TransformingSystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   * @param transformer default input stream transformer for this system
   */
  public TransformingSystemDescriptor(String systemName, String factoryClassName, InputTransformer<SystemTransformerType> transformer) {
    super(systemName, factoryClassName, transformer, null);
    if (transformer == null) {
      throw new SamzaException("Default InputTransformer may not be null for a TransformingSystemDescriptor");
    }
  }

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the default system level {@link InputTransformer}
   * <p>
   * The type of messages in the stream is the type of messages returned by the default system level
   * {@link InputTransformer}.
   *
   * @param streamId id of the input stream
   * @param serde stream level serde for the input stream
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract InputDescriptor<SystemTransformerType, ? extends InputDescriptor> getInputDescriptor(String streamId, Serde serde);
}
