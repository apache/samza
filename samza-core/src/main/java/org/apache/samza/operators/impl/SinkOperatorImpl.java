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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import java.util.Collection;


/**
 * An operator that sends incoming messages to an arbitrary output system using the provided {@link SinkFunction}.
 */
class SinkOperatorImpl<M> extends OperatorImpl<M, M> {

  private final SinkOperatorSpec<M> sinkOpSpec;
  private final SinkFunction<M> sinkFn;

  SinkOperatorImpl(SinkOperatorSpec<M> sinkOpSpec) {
    this.sinkOpSpec = sinkOpSpec;
    this.sinkFn = sinkOpSpec.getSinkFn();
  }

  @Override
  protected void handleInit(Context context) {
    this.sinkFn.init(context);
  }

  @Override
  protected CompletionStage<Collection<M>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    this.sinkFn.apply(message, collector, coordinator);
    return CompletableFuture.completedFuture(Collections.singleton(message));
  }

  @Override
  protected void handleClose() {
    this.sinkFn.close();
  }

  protected OperatorSpec<M, M> getOperatorSpec() {
    return sinkOpSpec;
  }
}
