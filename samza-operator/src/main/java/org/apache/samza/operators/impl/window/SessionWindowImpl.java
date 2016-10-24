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

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.operators.impl.SessionStateStoreImpl;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.internal.Trigger;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * Default implementation class of a {@link org.apache.samza.operators.internal.Operators.WindowOperator} for a session window.
 *
 * @param <M>  the type of input {@link org.apache.samza.operators.data.Message}
 * @param <RK>  the type of window key
 * @param <WS>  the type of window state
 * @param <RM>  the type of aggregated value of the window
 */
public class SessionWindowImpl<M extends Message, RK, WS extends WindowState, RM extends WindowOutput<RK, ?>> extends
    OperatorImpl<M, RM> {

  private final Operators.WindowOperator<M, RK, WS, RM> sessWnd;
  private SessionStateStoreImpl<M, RK, WS> wndStore = null;

  public SessionWindowImpl(Operators.WindowOperator<M, RK, WS, RM> sessWnd) {
    this.sessWnd = sessWnd;
  }


  @Override protected void init(MessageStream<M> source, TaskContext context) {
    this.wndStore = new SessionStateStoreImpl<>(this.sessWnd.getStoreFunctions(), sessWnd.getStoreName(source));
    this.wndStore.init(context);
  }

  @Override protected void onNext(M inputMessage, MessageCollector collector, TaskCoordinator coordinator) {
    this.wndStore.updateState(inputMessage);
    Entry<RK, WS> newEntry = this.wndStore.getState(inputMessage);
    Trigger trigger = sessWnd.getTrigger();
    if (trigger != null) {
      processTriggers(inputMessage, newEntry.getKey(), collector, coordinator);
    }
  }

  private void processTriggers(M inputMessage, RK windowKey, MessageCollector collector, TaskCoordinator coordinator) {
    Trigger<M, WS> trigger = sessWnd.getTrigger();
    WS windowState = wndStore.getState(inputMessage).getValue();

    BiFunction<M, WS, Boolean> earlyTrigger = trigger.getEarlyTrigger();
    Function<WS, Boolean> timerTrigger = trigger.getTimerTrigger();

    if ((earlyTrigger!=null && earlyTrigger.apply(inputMessage, windowState)) || (timerTrigger!=null && timerTrigger.apply(windowState))) {
      emitSessionOutput(windowKey, windowState.getOutputValue(), collector, coordinator);
      wndStore.delete(windowKey);
    }
  }

  private void emitSessionOutput(RK windowKey, Object windowValue, MessageCollector collector, TaskCoordinator coordinator) {
    WindowOutput<RK, ?> outputMessage = WindowOutput.of(windowKey, windowValue);
    this.nextProcessors((RM)outputMessage, collector, coordinator);
  }



  @Override
  public void onTimer(long nanoTime, MessageCollector collector, TaskCoordinator coordinator) {
    System.out.println("inside timer");
    // This is to periodically check the timeout triggers to get the list of window states to be updated
    KeyValueIterator<RK, WS> sessionIterator = wndStore.getIterator();

    while (sessionIterator.hasNext()) {
      Entry<RK, WS> openSession = sessionIterator.next();
      RK key = openSession.getKey();
      WS state = openSession.getValue();
      Function<WS, Boolean> timerTrigger = sessWnd.getTrigger().getTimerTrigger();
      if (timerTrigger.apply(state)) {
        emitSessionOutput(key, state.getOutputValue(), collector, coordinator);
        sessionIterator.remove();
      } else {
        break;
      }
    }
  }
}
