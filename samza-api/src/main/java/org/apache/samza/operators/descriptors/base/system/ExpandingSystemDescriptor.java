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
import org.apache.samza.operators.functions.StreamExpander;
import org.apache.samza.serializers.Serde;

/**
 * The base descriptor for a system that provides a default {@link StreamExpander} implementation for every
 * input stream, in addition to the system factory class name, and optionally a default system level serde and
 * an input stream transformer.
 *
 * @param <SystemExpanderType> default type of messages in the {@link StreamExpander} generated input stream
 * @param <SubClass> type of the concrete sub-class
 */
public abstract class ExpandingSystemDescriptor<SystemExpanderType, SubClass extends ExpandingSystemDescriptor<SystemExpanderType, SubClass>>
    extends SystemDescriptor<SubClass> {
  private final StreamExpander<SystemExpanderType> expander;
  private final InputTransformer transformer;

  /**
   * Constructs an {@link ExpandingSystemDescriptor} instance.
   *
   * @param systemName name of this system
   * @param factoryClassName name of the SystemFactory class for this system
   * @param transformer default input stream transformer for this system, or null
   * @param expander input stream expander for this system
   */
  public ExpandingSystemDescriptor(String systemName, String factoryClassName,
      InputTransformer transformer, StreamExpander<SystemExpanderType> expander) {
    super(systemName, factoryClassName);
    this.transformer = transformer;
    if (expander == null) {
      throw new SamzaException("Default StreamExpander may not be null for an ExpandingSystemDescriptor");
    }
    this.expander = expander;
  }

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the default system level {@link InputTransformer} if any.
   * <p>
   * The type of messages in the stream is the same as the type of messages in the {@code MessageStream} returned
   * by the {@link StreamExpander} for this system.
   *
   * @param streamId id of the input stream
   * @param serde stream level serde for the input stream
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract InputDescriptor<SystemExpanderType, ? extends InputDescriptor> getInputDescriptor(String streamId, Serde serde);

  /**
   * Gets a {@link InputDescriptor} for an input stream on this system. The stream has the provided
   * stream level serde, and the provided stream level {@link InputTransformer}.
   * <p>
   * The type of messages in the stream is the same as the type of messages in the {@code MessageStream} returned
   * by the {@link StreamExpander} for this system.
   *
   * @param streamId id of the input stream
   * @param serde stream level serde for the input stream
   * @param transformer stream level {@link InputTransformer} for the input stream
   * @return a {@link InputDescriptor} for the input stream
   */
  public abstract InputDescriptor<SystemExpanderType, ? extends InputDescriptor> getInputDescriptor(String streamId, InputTransformer transformer, Serde serde);

  public StreamExpander<SystemExpanderType> getExpander() {
    return this.expander;
  }

  public InputTransformer getTransformer() {
    return this.transformer;
  }
}
