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
package org.apache.samza.operators.impl.window;

import org.apache.samza.operators.api.MessageStream;
import org.apache.samza.operators.api.WindowState;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.operators.api.internal.Operators.WindowOperator;
import org.apache.samza.operators.api.internal.WindowOutput;
import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.operators.impl.StateStoreImpl;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * Default implementation class of a {@link WindowOperator} for a session window.
 *
 * @param <M>  the type of input {@link Message}
 * @param <RK>  the type of window key
 * @param <WS>  the type of window state
 * @param <RM>  the type of aggregated value of the window
 */
public class SessionWindowImpl<M extends Message, RK, WS extends WindowState, RM extends WindowOutput<RK, ?>> extends
    OperatorImpl<M, RM> {
  private final WindowOperator<M, RK, WS, RM> sessWnd;
  private StateStoreImpl<M, RK, WS> wndStore = null;

  public SessionWindowImpl(WindowOperator<M, RK, WS, RM> sessWnd) {
    this.sessWnd = sessWnd;
  }

  @Override protected void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    Entry<RK, WS> state = this.wndStore.getState(message);
    this.nextProcessors(this.sessWnd.getFunction().apply(message, state), collector, coordinator);
    this.wndStore.updateState(message, state);
  }

  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    // This is to periodically check the timeout triggers to get the list of window states to be updated
  }

  @Override protected void init(MessageStream<M> source, TaskContext context) {
    this.wndStore = new StateStoreImpl<>(this.sessWnd.getStoreFunctions(), sessWnd.getStoreName(source));
    this.wndStore.init(context);
  }
}
