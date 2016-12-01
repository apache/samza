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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.MessageStreamImpl;


/**
 * The spec for a sink operator that accepts user-defined logic to output a {@link MessageStreamImpl} to an external
 * system. This is a terminal operator and does allows further operator chaining.
 *
 * @param <M>  the type of input {@link MessageEnvelope}
 */
public class SinkOperatorSpec<M extends MessageEnvelope> implements OperatorSpec {

  /**
   * The user-defined sink function
   */
  private final SinkFunction<M> sinkFn;

  /**
   * Default constructor for a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output {@link MessageEnvelope},
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   */
  SinkOperatorSpec(SinkFunction<M> sinkFn) {
    this.sinkFn = sinkFn;
  }

  /**
   * This is a terminal operator and doesn't allow further operator chaining.
   * @return  null
   */
  @Override
  public MessageStreamImpl getOutputStream() {
    return null;
  }

  public SinkFunction<M> getSinkFn() {
    return this.sinkFn;
  }
}
