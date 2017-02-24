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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.triggers.Cancellable;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.TriggerContext;
import org.apache.samza.operators.triggers.TriggerImpl;
import org.apache.samza.operators.triggers.TriggerImpls;
import org.apache.samza.operators.util.InternalInMemoryStore;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

public class WindowOperatorImpl<M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<K, WV>> extends OperatorImpl<M, WM> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final PriorityQueue<TriggerTimerState> pendingCallbacks = new PriorityQueue<>();

  private final WindowInternal<M, K, WV> window;

  private final Map<WindowKey<K>, TriggerImpl> earlyTriggers = new HashMap<>();
  private final Map<WindowKey<K>, TriggerImpl> defaultTriggers = new HashMap<>();

  private MessageCollector recentCollector;
  private TaskCoordinator recentCoordinator;

  private final KeyValueStore<WindowKey<K>, WV> store = new InternalInMemoryStore<>();

  public WindowOperatorImpl(WindowOperatorSpec<M, WK, WV> spec) {
    window = spec.getWindow();
  }


  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    this.recentCollector = collector;
    this.recentCoordinator = coordinator;

    WindowKey<K> storeKey =  getStoreKey(message);
    BiFunction<M, WV, WV> foldFunction = window.getFoldFunction();
    WV wv = store.get(storeKey);
    WV newVal = foldFunction.apply(message, wv);
    store.put(storeKey, newVal);

    TriggerImpl earlyTriggerImpl = null;
    TriggerImpl defaultTriggerImpl = null;

    if (earlyTriggers.containsKey(storeKey)) {
      earlyTriggerImpl = earlyTriggers.get(storeKey);
      defaultTriggerImpl = defaultTriggers.get(storeKey);
    } else {
      TriggerContext earlyTriggerContext = new TriggerContextImpl(storeKey);;
      TriggerImpl.TriggerCallbackHandler earlyTriggerCallback = createTriggerHandler(storeKey);
      earlyTriggerImpl = TriggerImpls.createTriggerImpl(window.getEarlyTrigger(), earlyTriggerContext, earlyTriggerCallback);

      TriggerContext defaultTriggerContext = new TriggerContextImpl(storeKey);
      TriggerImpl.TriggerCallbackHandler defaultTriggerCallback = createTriggerHandler(storeKey);
      defaultTriggerImpl = TriggerImpls.createTriggerImpl(window.getDefaultTrigger(), defaultTriggerContext, defaultTriggerCallback);

      earlyTriggers.put(storeKey, earlyTriggerImpl);
      defaultTriggers.put(storeKey, defaultTriggerImpl);
    }

    earlyTriggerImpl.onMessage(message);
    defaultTriggerImpl.onMessage(message);
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    System.out.println("on timer..");

    TriggerTimerState state = pendingCallbacks.peek();
    if (state == null)
      return;

    long now = System.currentTimeMillis();
    long scheduleTimeMs;

    while(state != null && (scheduleTimeMs = state.getScheduleTimeMs()) < now) {
      pendingCallbacks.remove();
      System.out.println("callback now");
      state.getCallback().run();
      state = pendingCallbacks.peek();
    }
    super.propagateTimer(collector, coordinator);
  }

  private WindowKey<K> getStoreKey(M message) {
    Function<M, K> keyExtractor = window.getKeyExtractor();
    K key = null;

    if (keyExtractor != null) {
      key = keyExtractor.apply(message);
    }

    String windowId = null;

    if (window.getWindowType() == WindowType.TUMBLING) {
      long triggerDurationMs = ((TimeTrigger)window.getDefaultTrigger()).getDuration().toMillis();
      final long now = System.currentTimeMillis();
      Long windowBoundary = now - now % triggerDurationMs;
      windowId = windowBoundary.toString();
    }

    WindowKey<K> windowKey = new WindowKey(key, windowId);
    return windowKey;
  }

  private TriggerImpl.TriggerCallbackHandler createTriggerHandler(WindowKey<K> key) {
    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl impl, Object storeKey) {
        WindowKey<K> windowKey = (WindowKey<K>) storeKey;
        WV wv = store.get(windowKey);
        if (wv == null) {
          return;
        }
        WindowPane<K, WV> paneOutput = new WindowPane<>(windowKey, wv, window.getAccumulationMode());

        if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
          store.put(windowKey, null);
        }
        WindowOperatorImpl.super.propagateResult((WM)paneOutput, recentCollector, recentCoordinator);
      }
    };
    return handler;
  }


  private class TriggerContextImpl implements TriggerContext {
    private final Object windowKey;

    public TriggerContextImpl(Object windowKey) {
      this.windowKey = windowKey;
    }

    @Override
    public Cancellable scheduleCallback(Runnable listener, long durationMs) {
      TriggerTimerState timerState = new TriggerTimerState(windowKey, listener, durationMs);
      pendingCallbacks.add(timerState);
      return timerState;
    }

    @Override
    public Object getWindowKey() {
      return windowKey;
    }
  }

  private class TriggerTimerState implements Comparable<TriggerTimerState>, Cancellable {
    private final Object windowKey;
    private final Runnable callback;

    //the time in milliseconds at which the callback should trigger
    private final long scheduleTimeMs;

    public TriggerTimerState(Object windowKey, Runnable callback, long scheduleTimeMs) {
      this.windowKey = windowKey;
      this.callback = callback;
      this.scheduleTimeMs = scheduleTimeMs;
    }

    public Object getWindowKey() {
      return windowKey;
    }

    public Runnable getCallback() {
      return callback;
    }

    public long getScheduleTimeMs() {
      return scheduleTimeMs;
    }

    @Override
    public int compareTo(TriggerTimerState other) {
      return Long.compare(this.scheduleTimeMs, other.scheduleTimeMs);
    }

    @Override
    public boolean cancel() {
      return pendingCallbacks.remove(this);
    }
  }
}