// CHECKSTYLE:OFF
// Turn off checkstyle for this class because of a checkstyle bug in handling nested typed collections
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

import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.triggers.RepeatingTriggerImpl;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of a window operator that groups messages into finite windows for processing.
 *
 * This class implements the processing logic for various types of windows and triggers. It tracks and manages state for
 * all open windows, the active triggers that correspond to each of the windows and the pending callbacks. It provides
 * an implementation of {@link TriggerScheduler} that {@link TriggerImpl}s can use to schedule and cancel callbacks. It
 * also orchestrates the flow of messages through the various {@link TriggerImpl}s.
 *
 * <p> An instance of a {@link TriggerImplWrapper} is created corresponding to each {@link Trigger} configured for a
 * particular window. For every message added to the window, this class looks up the corresponding {@link TriggerImplWrapper}
 * for the trigger and invokes {@link TriggerImplWrapper#onMessage(TriggerKey, Object, MessageCollector, TaskCoordinator)}.
 * The {@link TriggerImplWrapper} maintains the {@link TriggerImpl} instance along with whether it has been canceled yet
 * or not. Then, the {@link TriggerImplWrapper} invokes onMessage on underlying its {@link TriggerImpl} instance. A
 * {@link TriggerImpl} instance is scoped to a window and its firing determines when results for its window are emitted. The
 * {@link WindowOperatorImpl} checks if the trigger fired, and propagates the result of the firing to its downstream
 * operators.
 *
 * @param <M> the type of the incoming message
 * @param <WK> the type of the key in this {@link org.apache.samza.operators.MessageStream}
 * @param <WV> the type of the value in the emitted window pane
 *
 */
public class WindowOperatorImpl<M, WK, WV> extends OperatorImpl<M, WindowPane<WK, WV>> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final WindowInternal<M, WK, WV> window;
  private final KeyValueStore<WindowKey<WK>, WindowState<WV>> store = new InternalInMemoryStore<>();
  TriggerScheduler<WK> triggerScheduler ;

  // The trigger state corresponding to each {@link TriggerKey}.
  private final Map<TriggerKey<WK>, TriggerImplWrapper> triggers = new HashMap<>();
  private final Clock clock;

  public WindowOperatorImpl(WindowOperatorSpec<M, WK, WV> spec, Clock clock) {
    this.clock = clock;
    this.window = spec.getWindow();
    this.triggerScheduler= new TriggerScheduler(clock);
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Processing message envelope: {}", message);

    WindowKey<WK> storeKey =  getStoreKey(message);
    WindowState<WV> existingState = store.get(storeKey);
    WindowState<WV> newState = applyFoldFunction(existingState, message);

    LOG.trace("New window value: {}, earliest timestamp: {}", newState.getWindowValue(), newState.getEarliestTimestamp());
    store.put(storeKey, newState);

    if (window.getEarlyTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.EARLY, storeKey);

      getOrCreateTriggerImplWrapper(triggerKey, window.getEarlyTrigger())
          .onMessage(triggerKey, message, collector, coordinator);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.DEFAULT, storeKey);
      getOrCreateTriggerImplWrapper(triggerKey, window.getDefaultTrigger())
          .onMessage(triggerKey, message, collector, coordinator);
    }
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    List<TriggerKey<WK>> keys = triggerScheduler.runPendingCallbacks();

    for (TriggerKey<WK> key : keys) {
      TriggerImplWrapper triggerImplWrapper = triggers.get(key);
      if (triggerImplWrapper != null) {
        triggerImplWrapper.onTimer(key, collector, coordinator);
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
    long earliestTimestamp;

    if (existingState == null) {
      LOG.trace("No existing state found for key");
      wv = window.getInitializer().get();
      earliestTimestamp = clock.currentTimeMillis();
    } else {
      wv = existingState.getWindowValue();
      earliestTimestamp = existingState.getEarliestTimestamp();
    }

    WV newVal = window.getFoldLeftFunction().apply(message, wv);
    WindowState<WV> newState = new WindowState(newVal, earliestTimestamp);

    return newState;
  }

  private TriggerImplWrapper getOrCreateTriggerImplWrapper(TriggerKey<WK> triggerKey, Trigger<M> trigger) {
    TriggerImplWrapper wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      LOG.trace("Returning existing trigger wrapper for {}", triggerKey);
      return wrapper;
    }

    LOG.trace("Creating a new trigger wrapper for {}", triggerKey);

    TriggerImpl<M, WK> triggerImpl = TriggerImpls.createTriggerImpl(trigger, clock, triggerKey);
    wrapper = new TriggerImplWrapper(triggerKey, triggerImpl);
    triggers.put(triggerKey, wrapper);

    return wrapper;
  }

  /**
   * Handles trigger firings, and propagates results to downstream operators.
   */
  private void onTriggerFired(TriggerKey<WK> triggerKey, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Trigger key {} fired." , triggerKey);

    TriggerImplWrapper wrapper = triggers.get(triggerKey);
    WindowKey<WK> windowKey = triggerKey.getKey();
    WindowState<WV> state = store.get(windowKey);

    if (state == null) {
      LOG.trace("No state found for triggerKey: {}", triggerKey);
      return;
    }

    WindowPane<WK, WV> paneOutput = computePaneOutput(triggerKey, state);
    super.propagateResult(paneOutput, collector, coordinator);

    // Handle accumulation modes.
    if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
      LOG.trace("Clearing state for trigger key: {}", triggerKey);
      store.put(windowKey, null);
    }

    // Cancel all early triggers too when the default trigger fires. Also, clean all state for the key.
    // note: We don't handle late arrivals yet, So, all arrivals are either early or on-time.
    if (triggerKey.getType() == FiringType.DEFAULT) {

      LOG.trace("Default trigger fired. Canceling triggers for {}", triggerKey);

      cancelTrigger(triggerKey, true);
      cancelTrigger(new TriggerKey(FiringType.EARLY, triggerKey.getKey()), true);
      WindowKey<WK> key = triggerKey.getKey();
      store.delete(key);
    }

    // Cancel non-repeating early triggers. All early triggers should be removed from the "triggers" map only after the
    // firing of their corresponding default trigger. Removing them pre-maturely (immediately after cancellation) will
    // will create a new {@link TriggerImplWrapper} instance at a future invocation of getOrCreateTriggerWrapper().
    // This would cause an already canceled trigger to fire again for the window.

    if (triggerKey.getType() == FiringType.EARLY && !wrapper.isRepeating()) {
      cancelTrigger(triggerKey, false);
    }
  }

  /**
   * Computes the pane output corresponding to a {@link TriggerKey} that fired.
   */
  private WindowPane<WK, WV> computePaneOutput(TriggerKey<WK> triggerKey, WindowState<WV> state) {
    WindowKey<WK> windowKey = triggerKey.getKey();
    WV windowVal = state.getWindowValue();

    // For session windows, we will create a new window key by using the time of the first message in the window as
    //the paneId.
    if (window.getWindowType() == WindowType.SESSION) {
      windowKey = new WindowKey<>(windowKey.getKey(), Long.toString(state.getEarliestTimestamp()));
    }

    // Make a defensive copy so that we are immune to further mutations on the collection
    if (windowVal instanceof Collection) {
      windowVal = (WV) new ArrayList<>((Collection<WV>) windowVal);
    }

    WindowPane<WK, WV> paneOutput = new WindowPane<>(windowKey, windowVal, window.getAccumulationMode(), triggerKey.getType());
    LOG.trace("Emitting pane output for trigger key {}", triggerKey);
    return paneOutput;
  }

  /**
   * Cancels the firing of the {@link TriggerImpl} identified by this {@link TriggerKey} and optionally removes it.
   */
  private void cancelTrigger(TriggerKey<WK> triggerKey, boolean shouldRemove) {
    TriggerImplWrapper wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      wrapper.cancel();
    }
    if (shouldRemove && triggerKey != null) {
      triggers.remove(triggerKey);
    }
  }

  /**
   * State corresponding to a created {@link TriggerImpl} instance.
   */
  private class TriggerImplWrapper {
    // The context, and the {@link TriggerImpl} instance corresponding to this triggerKey
    private final TriggerImpl<M, WK> impl;
    // Guard to ensure that we don't invoke onMessage or onTimer on already cancelled triggers
    private boolean isCancelled = false;

    public TriggerImplWrapper(TriggerKey<WK> key, TriggerImpl<M, WK> impl) {
      this.impl = impl;
    }

    public void onMessage(TriggerKey<WK> triggerKey, M message, MessageCollector collector, TaskCoordinator coordinator) {
      if (!isCancelled) {
        LOG.trace("Forwarding callbacks for {}", message);
        impl.onMessage(message, triggerScheduler);

        if (impl.shouldFire()) {
          // repeating trigger can trigger multiple times, So, clear the state to allow future triggerings.
          if (impl instanceof RepeatingTriggerImpl) {
            ((RepeatingTriggerImpl<M, WK>) impl).clear();
          }
          onTriggerFired(triggerKey, collector, coordinator);
        }
      }
    }

    public void onTimer(TriggerKey<WK> key, MessageCollector collector, TaskCoordinator coordinator) {
      if (impl.shouldFire() && !isCancelled) {
        LOG.trace("Triggering timer triggers");

        // repeating trigger can trigger multiple times, So, clear the trigger to allow future triggerings.
        if (impl instanceof RepeatingTriggerImpl) {
          ((RepeatingTriggerImpl<M, WK>) impl).clear();
        }
        onTriggerFired(key, collector, coordinator);
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

}