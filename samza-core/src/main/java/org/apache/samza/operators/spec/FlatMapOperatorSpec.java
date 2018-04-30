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

import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;


/**
 * The spec for an operator that transforms each input message to a collection of output messages. This is a package-private
 * class since the constructor of the class is only accessed via {@link OperatorSpecs} and the runtime reference to the object
 * is always upcast to {@link StreamOperatorSpec},
 *
 * @param <M> type of input message
 * @param <OM> type of output messages
 */
class FlatMapOperatorSpec<M, OM> extends StreamOperatorSpec<M, OM> {

  FlatMapOperatorSpec(FlatMapFunction<M, OM> flatMapFn, String opId) {
    super(flatMapFn, OpCode.FLAT_MAP, opId);
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.transformFn instanceof WatermarkFunction ? (WatermarkFunction) this.transformFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return this.transformFn instanceof TimerFunction ? (TimerFunction) this.transformFn : null;
  }
}
