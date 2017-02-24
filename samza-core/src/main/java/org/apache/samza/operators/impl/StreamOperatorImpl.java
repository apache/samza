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

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * A StreamOperator that accepts a 1:n transform function and applies it to each incoming message.
 *
 * @param <M>  type of message in the input stream
 * @param <RM>  type of message in the output stream
 */
class StreamOperatorImpl<M, RM> extends OperatorImpl<M, RM> {

  private final FlatMapFunction<M, RM> transformFn;

  StreamOperatorImpl(StreamOperatorSpec<M, RM> streamOperatorSpec, MessageStreamImpl<M> source, Config config, TaskContext context) {
    this.transformFn = streamOperatorSpec.getTransformFn();
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    // call the transform function and then for each output call propagateResult()
    this.transformFn.apply(message).forEach(r -> this.propagateResult(r, collector, coordinator));
  }
}
