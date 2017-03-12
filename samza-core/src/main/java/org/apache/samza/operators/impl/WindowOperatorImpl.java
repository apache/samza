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
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.triggers.Cancellable;
import org.apache.samza.operators.triggers.RepeatingTriggerImpl;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.TriggerContext;
import org.apache.samza.operators.triggers.TriggerImpl;
import org.apache.samza.operators.triggers.TriggerImpls;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.util.InternalInMemoryStore;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;

/**
 * Implementation of a window operator that groups messages into finite windows for processing.
 *
 * This class implements the processing logic for various types of windows and triggers. It tracks and manages state for
 * all open windows, the active triggers that correspond to each of the windows and the pending callbacks. It provides
 * an implementation of {@link TriggerContext} that {@link TriggerImpl}s can use to schedule and cancel callbacks. It
 * also orchestrates the flow of messages through the various {@link TriggerImpl}s.
 *
 * <p> An instance of a {@link TriggerImpl} is created corresponding to each {@link Trigger} configured for a window. For every
 * MessageEnvelope added to the window, this class invokes {@link TriggerImpl#onMessage(Object, TriggerContext)} on its
 * corresponding {@link TriggerImpl}s. A {@link TriggerImpl} instance is scoped to a window and its firing determines when
 * results for its window are emitted. The {@link WindowOperatorImpl} checks if the trigger fired, and looks
 * up the {@link TriggerImplState} corresponding to that firing. It then, propagates the result of the firing to its
 * downstream operators.
 *
 * @param <M>  the type of the incoming {@link MessageEnvelope}
 * @param <WK>  the type of the key in this {@link org.apache.samza.operators.MessageStream}
 * @param <WV> the type of the value in the emitted window pane
 *
 */
public class WindowOperatorImpl<M, WK, WV> extends OperatorImpl<M, WindowPane<WK, WV>> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  // Queue of pending callbacks. Callbacks are evaluated at every tick.
  private final PriorityQueue<TriggerCallbackState> pendingCallbacks = new PriorityQueue<>();
  private final WindowInternal<M, WK, WV> window;
  private final KeyValueStore<WindowKey<WK>, WindowState<WV>> store = new InternalInMemoryStore<>();

  // The trigger state corresponding to each {@link TriggerKey}.
  private final Map<TriggerKey<WK>, TriggerImplState> triggers = new HashMap<>();
  private final Clock clock;

  public WindowOperatorImpl(WindowOperatorSpec<M, WK, WV> spec, Clock clock) {
    this.clock = clock;
    this.window = spec.getWindow();
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    WindowKey<WK> storeKey =  getStoreKey(message);
    WindowState<WV> existingState = store.get(storeKey);
    WindowState<WV> newState = applyFoldFunction(existingState, message);

    store.put(storeKey, newState);

    if (window.getEarlyTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.EARLY, storeKey);

      getOrCreateTriggerImplState(triggerKey, window.getEarlyTrigger())
          .onMessage(message, collector, coordinator);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.DEFAULT, storeKey);
      getOrCreateTriggerImplState(triggerKey, window.getDefaultTrigger())
          .onMessage(message, collector, coordinator);
    }
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long now = clock.currentTimeMillis();
    TriggerCallbackState state;
    while ((state = pendingCallbacks.peek()) != null && state.getScheduledTimeMs() <= now) {
      pendingCallbacks.remove();
      state.getCallback().run();

      TriggerImplState triggerImplState = triggers.get(state.triggerKey);
      if (triggerImplState != null) {
        triggerImplState.onTimer(collector, coordinator);
      }
    }
  }

  /**
   * Get the key to be used for lookups in the store for this message.
   */
  private WindowKey<WK> getStoreKey(M message) {
    Function<M, WK> keyExtractor = window.getKeyExtractor();
    WK key = null;

    if (keyExtractor != null) {
      key = keyExtractor.apply(message);
    }

    String paneId = null;

    if (window.getWindowType() == WindowType.TUMBLING) {
      long triggerDurationMs = ((TimeTrigger<M>) window.getDefaultTrigger()).getDuration().toMillis();
      final long now = clock.currentTimeMillis();
      Long windowBoundary = now - now % triggerDurationMs;
      paneId = windowBoundary.toString();
    }

    return new WindowKey<>(key, paneId);
  }

  private WindowState<WV> applyFoldFunction(WindowState<WV> existingState, M message) {
    WV wv;
    long earliesttimestamp;

    if (existingState == null) {
      wv = window.getInitializer().get();
      earliesttimestamp = clock.currentTimeMillis();
    } else {
      wv = existingState.getWindowValue();
      earliesttimestamp = existingState.getEarliestRecvTime();
    }

    WV newVal = window.getFoldFunction().apply(message, wv);
    WindowState<WV> newState = new WindowState(newVal, earliesttimestamp);

    return newState;
  }

  private TriggerImplState getOrCreateTriggerImplState(TriggerKey<WK> triggerKey, Trigger<M> trigger) {
    TriggerImplState wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      return wrapper;
    }

    TriggerImpl<M> triggerImpl = TriggerImpls.createTriggerImpl(trigger, clock);
    TriggerContextImpl triggerContext = new TriggerContextImpl(triggerKey);
    wrapper = new TriggerImplState(triggerKey, triggerImpl, triggerContext);
    triggers.put(triggerKey, wrapper);

    return wrapper;
  }

  /**
   * Handles trigger firings, and propagates results to downstream operators.
   */
  private void onTriggerFired(TriggerKey<WK> triggerKey, MessageCollector collector, TaskCoordinator coordinator) {
    TriggerImplState wrapper = triggers.get(triggerKey);
    WindowKey<WK> windowKey = triggerKey.key;
    WindowState<WV> state = store.get(windowKey);

    if (state == null) {
      return;
    }

    WindowPane<WK, WV> paneOutput = computePaneOutput(triggerKey, state);
    super.propagateResult(paneOutput, collector, coordinator);

    // Handle accumulation modes.
    if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
      store.put(windowKey, null);
    }

    // Cancel all early triggers too when the default trigger fires. Also, clean all state for the key.
    // note: We don't handle late arrivals yet, So, all arrivals are either early or on-time.
    if (triggerKey.getType() == FiringType.DEFAULT) {
      cancelTrigger(triggerKey, true);
      cancelTrigger(new TriggerKey(FiringType.EARLY, triggerKey.getKey()), true);
      WindowKey<WK> key = triggerKey.key;
      store.delete(key);
    }

    // Cancel non-repeating early triggers.
    if (triggerKey.getType() == FiringType.EARLY && !wrapper.isRepeating()) {
      cancelTrigger(triggerKey, false);
    }
  }

  /**
   * Computes the pane output corresponding to a {@link TriggerKey} that fired.
   */
  private WindowPane<WK, WV> computePaneOutput(TriggerKey<WK> triggerKey, WindowState<WV> state) {
    WindowKey<WK> windowKey = triggerKey.key;
    WV windowVal = state.getWindowValue();

    // For session windows, we will create a new window key by using the time of the first message in the window as
    //the paneId.
    if (window.getWindowType() == WindowType.SESSION) {
      windowKey = new WindowKey<>(windowKey.getKey(), Long.toString(state.getEarliestRecvTime()));
    }

    // Make a defensive copy so that we are immune to further mutations on the collection
    if (windowVal instanceof Collection) {
      windowVal = (WV) new ArrayList<>((Collection<WV>) windowVal);
    }

    WindowPane<WK, WV> paneOutput = new WindowPane<>(windowKey, windowVal, window.getAccumulationMode(), triggerKey.getType());
    return paneOutput;
  }

  /**
   * Cancels the firing of the {@link TriggerImpl} identified by this {@link TriggerKey} and optionally removes it.
   */
  private void cancelTrigger(TriggerKey<WK> triggerKey, boolean shouldRemove) {
    TriggerImplState wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      wrapper.cancel();
    }
    if (shouldRemove && triggerKey != null) {
      triggers.remove(triggerKey);
    }
  }

  /**
   * Implementation of the {@link TriggerContext} that allows {@link TriggerImpl}s to schedule and cancel callbacks.
   */
  public class TriggerContextImpl implements TriggerContext {
    private final TriggerKey<WK> triggerKey;

    public TriggerContextImpl(TriggerKey<WK> triggerKey) {
      this.triggerKey = triggerKey;
    }

    @Override
    public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs) {
      TriggerCallbackState timerState = new TriggerCallbackState(triggerKey, runnable, callbackTimeMs);
      pendingCallbacks.add(timerState);
      return timerState;
    }
  }

  /**
   * State corresponding to pending timer callbacks scheduled by various {@link TriggerImpl}s.
   */
  private class TriggerCallbackState implements Comparable<TriggerCallbackState>, Cancellable {

    private final TriggerKey<WK> triggerKey;
    private final Runnable callback;

    // the time in milliseconds at which the callback should trigger
    private final long scheduledTimeMs;

    private TriggerCallbackState(TriggerKey<WK> triggerKey, Runnable callback, long scheduledTimeMs) {
      this.triggerKey = triggerKey;
      this.callback = callback;
      this.scheduledTimeMs = scheduledTimeMs;
    }

    private Runnable getCallback() {
      return callback;
    }

    private long getScheduledTimeMs() {
      return scheduledTimeMs;
    }

    @Override
    public int compareTo(TriggerCallbackState other) {
      return Long.compare(this.scheduledTimeMs, other.scheduledTimeMs);
    }

    @Override
    public boolean cancel() {
      return pendingCallbacks.remove(this);
    }
  }

  /**
   * State corresponding to a created {@link TriggerImpl} instance.
   */
  private class TriggerImplState {
    private final TriggerKey<WK> triggerKey;
    // The context, and the {@link TriggerImpl} instance corresponding to this triggerKey
    private final TriggerImpl<M> impl;
    private final TriggerContext context;
    // Guard to ensure that we don't invoke onMessage on already cancelled triggers
    private boolean isCancelled = false;

    public TriggerImplState(TriggerKey<WK> triggerKey, TriggerImpl<M> impl, TriggerContext context) {
      this.triggerKey = triggerKey;
      this.impl = impl;
      this.context = context;
    }

    public void onMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
      if (!isCancelled) {
        impl.onMessage(message, context);

        if (impl.shouldFire()) {
          // repeating trigger can trigger multiple times, So, clear the state to allow future triggerings.
          impl.clear();
          onTriggerFired(triggerKey, collector, coordinator);
        }
      }
    }

    public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
      if (impl.shouldFire() && !isCancelled) {
        // repeating trigger can trigger multiple times, So, clear the trigger to allow future triggerings.
        impl.clear();
        onTriggerFired(triggerKey, collector, coordinator);
      }
    }

    public void cancel() {
      impl.cancel();
      isCancelled = true;
    }

    public boolean isRepeating() {
      return this.impl instanceof RepeatingTriggerImpl;
    }
  }

  private static class TriggerKey<T> {
    private final FiringType type;
    private final WindowKey<T> key;

    public WindowKey<T> getKey() {
      return key;
    }

    public TriggerKey(FiringType type, WindowKey<T> key) {
      assert type != null;
      assert key != null;

      this.type = type;
      this.key = key;
    }

    /**
     * Equality is determined by both the type, and the window key.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TriggerKey<T> that = (TriggerKey<T>) o;
      return type == that.type && key.equals(that.key);

    }

    /**
     * Hashcode is computed by from the type, and the window key.
     */
    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + key.hashCode();
      return result;
    }

    public FiringType getType() {
      return type;
    }
  }
}