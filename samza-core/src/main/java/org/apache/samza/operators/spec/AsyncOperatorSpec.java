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

import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;


/**
 * The spec for an operator that transforms each input message to a collection of output messages.
 *
 * @param <M> type of input message
 * @param <OM> type of output messages
 */
public class AsyncOperatorSpec<M, OM> extends OperatorSpec<M, OM> {
  protected final AsyncFlatMapFunction<M, OM> transformFn;

  /**
   * Constructor for a {@link AsyncOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param opId  the unique ID for this {@link OperatorSpec}
   */
  AsyncOperatorSpec(AsyncFlatMapFunction<M, OM> transformFn, String opId) {
    super(OpCode.FLAT_MAP, opId);
    this.transformFn = transformFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.transformFn instanceof WatermarkFunction ? (WatermarkFunction) this.transformFn : null;
  }

  @Override
  public ScheduledFunction getScheduledFn() {
    return this.transformFn instanceof ScheduledFunction ? (ScheduledFunction) this.transformFn : null;
  }

  public AsyncFlatMapFunction<M, OM> getTransformFn() {
    return this.transformFn;
  }
}
