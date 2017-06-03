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

import org.apache.samza.config.Config;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.triggers.RepeatingTriggerImpl;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
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
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Implementation of a window operator that groups messages into finite windows for processing.
 *
 * This class implements the processing logic for various types of windows and triggers. It tracks and manages state for
 * all open windows, the active triggers that correspond to each of the windows and the pending callbacks. It provides
 * an implementation of {@link TriggerScheduler} that {@link TriggerImpl}s can use to schedule and cancel callbacks. It
 * also orchestrates the flow of messages through the various {@link TriggerImpl}s.
 *
 * <p> An instance of a {@link TriggerImplHandler} is created corresponding to each {@link Trigger} configured for a
 * particular window. For every message added to the window, this class looks up the corresponding {@link TriggerImplHandler}
 * for the trigger and invokes {@link TriggerImplHandler#onMessage(TriggerKey, Object, MessageCollector, TaskCoordinator)}.
 * The {@link TriggerImplHandler} maintains the {@link TriggerImpl} instance along with whether it has been canceled yet
 * or not. Then, the {@link TriggerImplHandler} invokes onMessage on underlying its {@link TriggerImpl} instance. A
 * {@link TriggerImpl} instance is scoped to a window and its firing determines when results for its window are emitted.
 * The {@link WindowOperatorImpl} checks if the trigger fired and returns the result of the firing.
 *
 * @param <M> the type of the incoming message
 * @param <WK> the type of the key in this {@link org.apache.samza.operators.MessageStream}
 * @param <WV> the type of the value in the emitted window pane
 *
 */
public class WindowOperatorImpl<M, WK, WV> extends OperatorImpl<M, WindowPane<WK, WV>> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final WindowOperatorSpec<M, WK, WV> windowOpSpec;
  private final Clock clock;
  private final WindowInternal<M, WK, WV> window;
  private final KeyValueStore<WindowKey<WK>, WindowState<WV>> store = new InternalInMemoryStore<>();
  private TriggerScheduler<WK> triggerScheduler;

  // The trigger state corresponding to each {@link TriggerKey}.
  private final Map<TriggerKey<WK>, TriggerImplHandler> triggers = new HashMap<>();

  public WindowOperatorImpl(WindowOperatorSpec<M, WK, WV> windowOpSpec, Clock clock) {
    this.windowOpSpec = windowOpSpec;
    this.clock = clock;
    this.window = windowOpSpec.getWindow();
    this.triggerScheduler= new TriggerScheduler(clock);
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    WindowInternal<M, WK, WV> window = windowOpSpec.getWindow();
    if (window.getFoldLeftFunction() != null) {
      window.getFoldLeftFunction().init(config, context);
    }
  }

  @Override
  public Collection<WindowPane<WK, WV>> handleMessage(
      M message, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Processing message envelope: {}", message);
    List<WindowPane<WK, WV>> results = new ArrayList<>();

    WindowKey<WK> storeKey =  getStoreKey(message);
    WindowState<WV> existingState = store.get(storeKey);
    LOG.trace("Store key ({}) has existing state ({})", storeKey, existingState);
    WindowState<WV> newState = applyFoldFunction(existingState, message);

    LOG.trace("New window value: {}, earliest timestamp: {}",
        newState.getWindowValue(), newState.getEarliestTimestamp());
    store.put(storeKey, newState);

    if (window.getEarlyTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.EARLY, storeKey);

      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getEarlyTrigger());
      Optional<WindowPane<WK, WV>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<WK> triggerKey = new TriggerKey<>(FiringType.DEFAULT, storeKey);
      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getDefaultTrigger());
      Optional<WindowPane<WK, WV>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    return results;
  }

  @Override
  public Collection<WindowPane<WK, WV>> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
    List<WindowPane<WK, WV>> results = new ArrayList<>();

    List<TriggerKey<WK>> keys = triggerScheduler.runPendingCallbacks();

    for (TriggerKey<WK> key : keys) {
      TriggerImplHandler triggerImplHandler = triggers.get(key);
      if (triggerImplHandler != null) {
        Optional<WindowPane<WK, WV>> maybeTriggeredPane = triggerImplHandler.onTimer(key, collector, coordinator);
        maybeTriggeredPane.ifPresent(results::add);
      }
    }

    return results;
  }

  @Override
  protected OperatorSpec<WindowPane<WK, WV>> getOperatorSpec() {
    return windowOpSpec;
  }

  @Override
  protected void handleClose() {
    WindowInternal<M, WK, WV> window = windowOpSpec.getWindow();

    if (window.getFoldLeftFunction() != null) {
      window.getFoldLeftFunction().close();
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
      LOG.trace("No existing state found for key. Invoking initializer.");
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

  private TriggerImplHandler getOrCreateTriggerImplHandler(TriggerKey<WK> triggerKey, Trigger<M> trigger) {
    TriggerImplHandler wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      LOG.trace("Returning existing trigger wrapper for {}", triggerKey);
      return wrapper;
    }

    LOG.trace("Creating a new trigger wrapper for {}", triggerKey);

    TriggerImpl<M, WK> triggerImpl = TriggerImpls.createTriggerImpl(trigger, clock, triggerKey);
    wrapper = new TriggerImplHandler(triggerKey, triggerImpl);
    triggers.put(triggerKey, wrapper);

    return wrapper;
  }

  /**
   * Handles trigger firings and returns the optional result.
   */
  private Optional<WindowPane<WK, WV>> onTriggerFired(
      TriggerKey<WK> triggerKey, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Trigger key {} fired." , triggerKey);

    TriggerImplHandler wrapper = triggers.get(triggerKey);
    WindowKey<WK> windowKey = triggerKey.getKey();
    WindowState<WV> state = store.get(windowKey);

    if (state == null) {
      LOG.trace("No state found for triggerKey: {}", triggerKey);
      return Optional.empty();
    }

    WindowPane<WK, WV> paneOutput = computePaneOutput(triggerKey, state);

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

    return Optional.of(paneOutput);
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

    WindowPane<WK, WV> paneOutput =
        new WindowPane<>(windowKey, windowVal, window.getAccumulationMode(), triggerKey.getType());
    LOG.trace("Emitting pane output for trigger key {}", triggerKey);
    return paneOutput;
  }

  /**
   * Cancels the firing of the {@link TriggerImpl} identified by this {@link TriggerKey} and optionally removes it.
   */
  private void cancelTrigger(TriggerKey<WK> triggerKey, boolean shouldRemove) {
    TriggerImplHandler triggerImplHandler = triggers.get(triggerKey);
    if (triggerImplHandler != null) {
      triggerImplHandler.cancel();
    }
    if (shouldRemove && triggerKey != null) {
      triggers.remove(triggerKey);
    }
  }

  /**
   * State corresponding to a created {@link TriggerImpl} instance.
   */
  private class TriggerImplHandler {
    // The context, and the {@link TriggerImpl} instance corresponding to this triggerKey
    private final TriggerImpl<M, WK> impl;
    // Guard to ensure that we don't invoke onMessage or onTimer on already cancelled triggers
    private boolean isCancelled = false;

    public TriggerImplHandler(TriggerKey<WK> key, TriggerImpl<M, WK> impl) {
      this.impl = impl;
    }

    public Optional<WindowPane<WK, WV>> onMessage(TriggerKey<WK> triggerKey, M message,
        MessageCollector collector, TaskCoordinator coordinator) {
      if (!isCancelled) {
        LOG.trace("Forwarding callbacks for {}", message);
        impl.onMessage(message, triggerScheduler);

        if (impl.shouldFire()) {
          // repeating trigger can trigger multiple times, So, clear the state to allow future triggerings.
          if (impl instanceof RepeatingTriggerImpl) {
            ((RepeatingTriggerImpl<M, WK>) impl).clear();
          }
          return onTriggerFired(triggerKey, collector, coordinator);
        }
      }
      return Optional.empty();
    }

    public Optional<WindowPane<WK, WV>> onTimer(
        TriggerKey<WK> key, MessageCollector collector, TaskCoordinator coordinator) {
      if (impl.shouldFire() && !isCancelled) {
        LOG.trace("Triggering timer triggers");

        // repeating trigger can trigger multiple times, So, clear the trigger to allow future triggerings.
        if (impl instanceof RepeatingTriggerImpl) {
          ((RepeatingTriggerImpl<M, WK>) impl).clear();
        }
        return onTriggerFired(key, collector, coordinator);
      }
      return Optional.empty();
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