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

import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;


/**
 * The spec for an operator that outputs a stream to an arbitrary external system.
 * <p>
 * This is a terminal operator and does not allow further operator chaining.
 *
 * @param <M>  the type of input message
 */
public class SinkOperatorSpec<M> extends OperatorSpec<M, Void> {

  private final SinkFunction<M> sinkFn;

  /**
   * Constructs a {@link SinkOperatorSpec} with a user defined {@link SinkFunction}.
   *
   * @param sinkFn  a user defined {@link SinkFunction} that will be called with the output message,
   *                the output {@link org.apache.samza.task.MessageCollector} and the
   *                {@link org.apache.samza.task.TaskCoordinator}.
   * @param opId  the unique ID of this {@link OperatorSpec} in the graph
   */
  SinkOperatorSpec(SinkFunction<M> sinkFn, String opId) {
    super(OpCode.SINK, opId);
    this.sinkFn = sinkFn;
  }

  public SinkFunction<M> getSinkFn() {
    return this.sinkFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return sinkFn instanceof WatermarkFunction ? (WatermarkFunction) sinkFn : null;
  }

  @Override
  public ScheduledFunction getScheduledFn() {
    return sinkFn instanceof ScheduledFunction ? (ScheduledFunction) sinkFn : null;
  }
}
