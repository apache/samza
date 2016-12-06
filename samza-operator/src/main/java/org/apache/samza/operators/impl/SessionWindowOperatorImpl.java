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

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StateStoreImpl;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.spec.WindowState;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * Default implementation class of a {@link WindowOperatorSpec} for a session window.
 *
 * @param <M>  the type of input {@link MessageEnvelope}
 * @param <RK>  the type of window key
 * @param <WS>  the type of window state
 * @param <RM>  the type of aggregated value of the window
 */
class SessionWindowOperatorImpl<M extends MessageEnvelope, RK, WS extends WindowState, RM extends WindowOutput<RK, ?>>
    extends OperatorImpl<M, RM> {

  private final WindowOperatorSpec<M, RK, WS, RM> windowSpec;
  private StateStoreImpl<M, RK, WS> stateStore = null;

  SessionWindowOperatorImpl(WindowOperatorSpec<M, RK, WS, RM> windowSpec) {
    this.windowSpec = windowSpec;
  }

  @Override
  public void init(MessageStream<M> source, TaskContext context) {
    this.stateStore = new StateStoreImpl<>(this.windowSpec.getStoreFns(), windowSpec.getStoreName(source));
    this.stateStore.init(context);
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    Entry<RK, WS> state = this.stateStore.getState(message);
    this.propagateResult(this.windowSpec.getTransformFn().apply(message, state), collector, coordinator);
    this.stateStore.updateState(message, state);
  }

  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    // This is to periodically check the timeout triggers to get the list of window states to be updated
  }
}
