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
import org.apache.samza.operators.triggers.RepeatingTriggerImpl;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of a window operator that groups messages into finite windows for processing.
 *
 * <p>Notes:
 * This class implements the processing logic for various types of windows and triggers. It tracks and manages state for
 * all open windows, the active triggers that correspond to each of the windows and the pending callbacks. It provides
 * an implementation of {@link TriggerContext} that {@link TriggerImpl}s can use to schedule and cancel callbacks. It
 * also orchestrates the flow of messages through the various {@link TriggerImpl}s.
 *
 * @param <M>  the type of the incoming {@link MessageEnvelope}
 * @param <K>  the type of the key in this {@link org.apache.samza.operators.MessageStream}
 * @param <WK> the type of the key in the emitted window pane
 * @param <WV> the type of the value in the emitted window pane
 * @param <WM> the type of the emitted window pane
 *
 * TODO: Implement expiration of entries and triggers
 */
public class WindowOperatorImpl<M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<K, WV>> extends OperatorImpl<M, WM> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final PriorityQueue<TriggerTimerState> pendingCallbacks = new PriorityQueue<>();
  private final WindowInternal<M, K, WV> window;
  private final KeyValueStore<WindowKey<K>, WV> store = new InternalInMemoryStore<>();

  private final Map<WindowKey<K>, TriggerImpl> earlyTriggers = new HashMap<>();
  private final Map<WindowKey<K>, TriggerImpl> defaultTriggers = new HashMap<>();

  private enum TriggerType { EARLY, DEFAULT, LATE }

  private MessageCollector recentCollector;
  private TaskCoordinator recentCoordinator;


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
      // lookup and propagate messages to the right triggers.
      TriggerContext earlyTriggerContext = new TriggerContextImpl(storeKey);
      TriggerImpl.TriggerCallbackHandler earlyTriggerHandler = createTriggerHandler(storeKey, TriggerType.EARLY);
      earlyTriggerImpl = TriggerImpls.createTriggerImpl(window.getEarlyTrigger(), earlyTriggerContext, earlyTriggerHandler);

      TriggerContext defaultTriggerContext = new TriggerContextImpl(storeKey);
      TriggerImpl.TriggerCallbackHandler defaultTriggerHandler = createTriggerHandler(storeKey, TriggerType.DEFAULT);
      defaultTriggerImpl = TriggerImpls.createTriggerImpl(window.getDefaultTrigger(), defaultTriggerContext, defaultTriggerHandler);

      earlyTriggers.put(storeKey, earlyTriggerImpl);
      defaultTriggers.put(storeKey, defaultTriggerImpl);
    }

    earlyTriggerImpl.onMessage(message);
    defaultTriggerImpl.onMessage(message);
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    this.recentCollector = collector;
    this.recentCoordinator = coordinator;

    TriggerTimerState state = pendingCallbacks.peek();
    if (state == null) {
      return;
    }

    long now = System.currentTimeMillis();

    while (state != null && state.getScheduleTimeMs() < now) {
      pendingCallbacks.remove();
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
      long triggerDurationMs = ((TimeTrigger) window.getDefaultTrigger()).getDuration().toMillis();
      final long now = System.currentTimeMillis();
      Long windowBoundary = now - now % triggerDurationMs;
      windowId = windowBoundary.toString();
    }

    WindowKey<K> windowKey = new WindowKey(key, windowId);
    return windowKey;
  }

  private TriggerImpl.TriggerCallbackHandler createTriggerHandler(WindowKey<K> key, TriggerType type) {
    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl impl, Object key) {

        // Remove default triggers and non-repeating early triggers for consideration in future callbacks.
        if (type == TriggerType.DEFAULT) {
          TriggerImpl defaultTrigger = defaultTriggers.get(key);
          defaultTrigger.onCancel();
          defaultTriggers.remove(key);
        } else if (type == TriggerType.EARLY && !(impl instanceof RepeatingTriggerImpl)) {
          TriggerImpl earlyTrigger = earlyTriggers.get(key);
          earlyTrigger.onCancel();
          earlyTriggers.remove(key);
        }

        WindowKey<K> windowKey = (WindowKey<K>) key;
        WV wv = store.get(windowKey);
        if (wv == null) {
          return;
        }

        WindowPane<K, WV> paneOutput = new WindowPane<>(windowKey, wv, window.getAccumulationMode());
        // Handle accumulation modes.
        if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
          store.put(windowKey, null);
        }
        System.out.println("inside store: " + ((Collection)paneOutput.getMessage()).size());

        if (paneOutput.getMessage() instanceof Collection) {
          WV valCopy = (WV)new ArrayList<M>((Collection)paneOutput.getMessage());
          paneOutput = new WindowPane(windowKey, valCopy, window.getAccumulationMode());
        }

        WindowOperatorImpl.super.propagateResult((WM) paneOutput, recentCollector, recentCoordinator);
      }
    };
    return handler;
  }

  /**
   * Implementation of the {@link TriggerContext} that allows {@link TriggerImpl}s to schedule and cancel callbacks.
   */
  private class TriggerContextImpl implements TriggerContext {
    private final Object windowKey;

    public TriggerContextImpl(Object windowKey) {
      this.windowKey = windowKey;
    }

    @Override
    public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs) {
      TriggerTimerState timerState = new TriggerTimerState(windowKey, runnable, callbackTimeMs);
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

    private TriggerTimerState(Object windowKey, Runnable callback, long scheduleTimeMs) {
      this.windowKey = windowKey;
      this.callback = callback;
      this.scheduleTimeMs = scheduleTimeMs;
    }

    private Object getWindowKey() {
      return windowKey;
    }

    private Runnable getCallback() {
      return callback;
    }

    private long getScheduleTimeMs() {
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