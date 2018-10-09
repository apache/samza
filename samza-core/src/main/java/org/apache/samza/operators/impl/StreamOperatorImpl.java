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
package org.apache.samza.operators.impl;

import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;


/**
 * A simple operator that accepts a 1:n transform function and applies it to each incoming message.
 *
 * @param <M>  the type of input message
 * @param <RM>  the type of result
 */
class StreamOperatorImpl<M, RM> extends OperatorImpl<M, RM> {

  private final StreamOperatorSpec<M, RM> streamOpSpec;
  private final FlatMapFunction<M, RM> transformFn;

  StreamOperatorImpl(StreamOperatorSpec<M, RM> streamOpSpec) {
    this.streamOpSpec = streamOpSpec;
    this.transformFn = streamOpSpec.getTransformFn();
  }

  @Override
  protected void handleInit(Context context) {
    transformFn.init(context);
  }

  @Override
  public Collection<RM> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    return this.transformFn.apply(message);
  }

  @Override
  protected void handleClose() {
    this.transformFn.close();
  }

  protected OperatorSpec<M, RM> getOperatorSpec() {
    return streamOpSpec;
  }
}
