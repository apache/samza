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

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.spec.AsyncFlatMapOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class AsyncFlatmapOperatorImpl<M, RM> extends OperatorImpl<M, RM> {
  private final AsyncFlatMapOperatorSpec<M, RM> opSpec;
  private final AsyncFlatMapFunction<M, RM> transformFn;

  AsyncFlatmapOperatorImpl(AsyncFlatMapOperatorSpec<M, RM> opSpec) {
    this.opSpec = opSpec;
    this.transformFn = opSpec.getTransformFn();
  }
  @Override
  protected void handleInit(Context context) {
    this.transformFn.init(context);
  }

  @Override
  protected CompletionStage<Collection<RM>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    return transformFn.apply(message);
  }

  @Override
  protected void handleClose() {
  }

  @Override
  protected OperatorSpec<M, RM> getOperatorSpec() {
    return opSpec;
  }
}
