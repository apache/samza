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
import org.apache.samza.operators.triggers.Trigger;
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

  private final Map<TriggerKey<K>, TriggerImplWrapper> triggers = new HashMap<>();

  private enum TriggerType { EARLY, DEFAULT, LATE }

  public WindowOperatorImpl(WindowOperatorSpec<M, WK, WV> spec) {
    window = spec.getWindow();
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    WindowKey<K> storeKey =  getStoreKey(message);
    System.out.println("===========================");
    System.out.println("processing message " + storeKey);

    BiFunction<M, WV, WV> foldFunction = window.getFoldFunction();
    WV wv = store.get(storeKey);

    if (wv == null) {
      wv = window.getInitializer().get();
    }

    WV newVal = foldFunction.apply(message, wv);
    store.put(storeKey, newVal);

    if (window.getEarlyTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(TriggerType.EARLY, storeKey);
      getOrCreateTriggerWrapper(triggerKey, window.getEarlyTrigger())
          .onMessage(message, collector, coordinator);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(TriggerType.DEFAULT, storeKey);
      getOrCreateTriggerWrapper(triggerKey, window.getDefaultTrigger())
          .onMessage(message, collector, coordinator);
    }
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long now = System.currentTimeMillis();
    TriggerTimerState state;
    while ((state = pendingCallbacks.peek()) != null && state.getScheduleTimeMs() < now) {
      pendingCallbacks.remove();
      state.getCallback().run();

      // TODO: we should probably clear out associated callbacks eagerly -
      // I don't think we're currently doing that.
      TriggerImplWrapper wrapper = triggers.get(state.triggerKey);
      if (wrapper != null) {
        wrapper.onTimer(collector, coordinator);
      }
    }
  }

  private WindowKey<K> getStoreKey(M message) {
    Function<M, K> keyExtractor = window.getKeyExtractor();
    K key = null;

    if (keyExtractor != null) {
      key = keyExtractor.apply(message);
    }

    String windowId = null;
    if (window.getWindowType() == WindowType.TUMBLING) {
      long triggerDurationMs = ((TimeTrigger<M>) window.getDefaultTrigger()).getDuration().toMillis();
      final long now = System.currentTimeMillis();
      Long windowBoundary = now - now % triggerDurationMs;
      windowId = windowBoundary.toString();
    }

    return new WindowKey<>(key, windowId);
  }

  private TriggerImplWrapper getOrCreateTriggerWrapper(TriggerKey<K> triggerKey, Trigger<M> trigger) {
    TriggerImplWrapper wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      System.out.println("FOUND returning : " + wrapper.impl + " " + wrapper + " " + triggerKey + " " + triggerKey.getKey() + " " + triggerKey.getType());
      return wrapper;
    }
    System.out.println("CREATE new returning : " + " " + wrapper + " " + triggerKey + " " + triggerKey.getKey() + " " + triggerKey.getType());

    TriggerHandlerImpl triggerHandler = new TriggerHandlerImpl();
    TriggerImpl<M> triggerImpl = TriggerImpls.createTriggerImpl(trigger);
    TriggerContextImpl triggerContext = new TriggerContextImpl(triggerKey);
    wrapper = new TriggerImplWrapper(triggerKey, triggerImpl, triggerContext, triggerHandler);
    triggers.put(triggerKey, wrapper);

    return wrapper;
  }

  private void handleTrigger(TriggerKey<K> triggerKey, MessageCollector collector, TaskCoordinator coordinator) {
    TriggerImplWrapper wrapper = triggers.get(triggerKey);

    if (!(wrapper.impl instanceof RepeatingTriggerImpl) || triggerKey.getType() == TriggerType.DEFAULT) {
      cancelTrigger(triggerKey);
      // for default trigger, also cancel the corresponding early trigger for the key.
      if (triggerKey.getType() == TriggerType.DEFAULT) {
        TriggerKey<K> earlyTriggerKey = new TriggerKey<>(TriggerType.EARLY, triggerKey.getKey());
        cancelTrigger(earlyTriggerKey);
      }
    }

    WindowKey<K> windowKey = triggerKey.key;
    WV wv = store.get(windowKey);

    if (wv == null) {
      return;
    }

    WindowPane<K, WV> paneOutput = new WindowPane<>(windowKey, wv, window.getAccumulationMode());
    // Handle accumulation modes.
    if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
      store.put(windowKey, null);
    }

    if (paneOutput.getMessage() instanceof Collection) {
      WV valCopy = (WV) new ArrayList<>((Collection<WV>) paneOutput.getMessage());
      paneOutput = new WindowPane(windowKey, valCopy, window.getAccumulationMode());
    }

    WindowOperatorImpl.super.propagateResult((WM) paneOutput, collector, coordinator);
  }

  private void cancelTrigger(TriggerKey<K> triggerKey) {
    TriggerImplWrapper wrapper = triggers.remove(triggerKey);
    if (wrapper != null) {
      wrapper.impl.cancel();
    }
  }

  /**
   * Implementation of the {@link TriggerContext} that allows {@link TriggerImpl}s to schedule and cancel callbacks.
   */
  public class TriggerContextImpl implements TriggerContext {
    private final TriggerKey<K> triggerKey;

    public TriggerContextImpl(TriggerKey<K> triggerKey) {
      this.triggerKey = triggerKey;
    }

    @Override
    public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs) {
      TriggerTimerState timerState = new TriggerTimerState(triggerKey, runnable, callbackTimeMs);
      pendingCallbacks.add(timerState);
      return timerState;
    }
  }

  private static class TriggerHandlerImpl implements TriggerImpl.TriggerCallbackHandler {
    private boolean triggered;

    @Override
    public void onTrigger() {
      triggered = true;
    }

    public boolean isTriggered() {
      return triggered;
    }

    public void clearTrigger() { triggered = false; }
  }

  private class TriggerTimerState implements Comparable<TriggerTimerState>, Cancellable {
    private final TriggerKey<K> triggerKey;
    private final Runnable callback;

    //the time in milliseconds at which the callback should trigger
    private final long scheduleTimeMs;

    private TriggerTimerState(TriggerKey<K> triggerKey, Runnable callback, long scheduleTimeMs) {
      this.triggerKey = triggerKey;
      this.callback = callback;
      this.scheduleTimeMs = scheduleTimeMs;
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

  private class TriggerImplWrapper {
    private final TriggerKey<K> triggerKey;
    private final TriggerImpl<M> impl;
    private final TriggerContext context;
    private final TriggerHandlerImpl handler;

    public TriggerImplWrapper(TriggerKey<K> triggerKey, TriggerImpl<M> impl, TriggerContext context, TriggerHandlerImpl handler) {
      this.triggerKey = triggerKey;
      this.impl = impl;
      this.context = context;
      this.handler = handler;
    }

    public void onMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
      impl.onMessage(message, context, handler);
      if (handler.isTriggered()) {
        //repeating trigger can trigger multiple times, So, clear the handler to allow future triggerings.
        handler.clearTrigger();
        handleTrigger(triggerKey, collector, coordinator);
      }
    }

    public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
      if (handler.isTriggered()) {
        //repeating trigger can trigger multiple times, So, clear the handler to allow future triggerings.
        handler.clearTrigger();
        handleTrigger(triggerKey, collector, coordinator);
      }
    }
  }

  private static class TriggerKey<T> {
    private final TriggerType type;
    private final WindowKey<T> key;

    public WindowKey<T> getKey() {
      return key;
    }

    public TriggerKey(TriggerType type, WindowKey<T> key) {
      assert type != null;
      assert key != null;

      this.type = type;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TriggerKey<T> that = (TriggerKey<T>) o;
      return type == that.type && key.equals(that.key);

    }

    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + key.hashCode();
      return result;
    }

    public TriggerType getType() {
      return type;
    }
  }
}