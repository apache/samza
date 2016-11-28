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
package org.apache.samza.operators.spec;

import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.MessageStreamImpl;


/**
 * The spec for a linear stream operator that outputs 0 or more {@link Message}s for each input {@link Message}.
 *
 * @param <M>  the type of input {@link Message}
 * @param <OM>  the type of output {@link Message}
 */
public class StreamOperatorSpec<M extends Message, OM extends Message> implements OperatorSpec<OM> {

  private final MessageStreamImpl<OM> outputStream;

  private final FlatMapFunction<M, OM> transformFn;

  /**
   * Default constructor for a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function that transforms each input {@link Message} into a collection
   *                     of output {@link Message}s
   */
  StreamOperatorSpec(FlatMapFunction<M, OM> transformFn) {
    this(transformFn, new MessageStreamImpl<>());
  }

  /**
   * Constructor for a {@link StreamOperatorSpec} that accepts an output {@link MessageStreamImpl}.
   *
   * @param transformFn  the transformation function
   * @param outputStream  the output {@link MessageStreamImpl}
   */
  StreamOperatorSpec(FlatMapFunction<M, OM> transformFn, MessageStreamImpl<OM> outputStream) {
    this.outputStream = outputStream;
    this.transformFn = transformFn;
  }

  @Override
  public MessageStreamImpl<OM> getOutputStream() {
    return this.outputStream;
  }

  public FlatMapFunction<M, OM> getTransformFn() {
    return this.transformFn;
  }
}
