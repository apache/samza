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

import com.google.common.base.Preconditions;
import org.apache.samza.config.Config;
import org.apache.samza.operators.impl.store.TimeSeriesKey;
import org.apache.samza.operators.impl.store.TimeSeriesStore;
import org.apache.samza.operators.impl.store.TimeSeriesStoreImpl;
import org.apache.samza.operators.impl.store.TimestampedValue;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.triggers.RepeatingTriggerImpl;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.TriggerImpl;
import org.apache.samza.operators.triggers.TriggerImpls;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.storage.kv.ClosableIterator;
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
 * an implementation of {@link TriggerScheduler} that {@link org.apache.samza.operators.triggers.TriggerImpl}s can use
 * to schedule and cancel callbacks. It also orchestrates the flow of messages through the various
 * {@link org.apache.samza.operators.triggers.TriggerImpl}s.
 *
 * <p> An instance of a {@link TriggerImplHandler} is created corresponding to each {@link org.apache.samza.operators.triggers.Trigger}
 * configured for a particular window. For every message added to the window, this class looks up the corresponding {@link TriggerImplHandler}
 * for the trigger and invokes {@link TriggerImplHandler#onMessage(TriggerKey, Object, MessageCollector, TaskCoordinator)}
 * The {@link TriggerImplHandler} maintains the {@link org.apache.samza.operators.triggers.TriggerImpl} instance along with
 * whether it has been canceled yet or not. Then, the {@link TriggerImplHandler} invokes onMessage on underlying its
 * {@link org.apache.samza.operators.triggers.TriggerImpl} instance. A {@link org.apache.samza.operators.triggers.TriggerImpl}
 * instance is scoped to a window and its firing determines when results for its window are emitted.
 *
 * The {@link WindowOperatorImpl} checks if the trigger fired and returns the result of the firing.
 *
 * @param <M> the type of the incoming message
 * @param <K> the type of the key in this {@link org.apache.samza.operators.MessageStream}
 *
 */
public class WindowOperatorImpl<M, K> extends OperatorImpl<M, WindowPane<K, Object>> {
  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final WindowOperatorSpec<M, K, Object> windowOpSpec;
  private final Clock clock;
  private final WindowInternal<M, K, Object> window;

  private TimeSeriesStore<K, Object> timeSeriesDb;
  private TriggerScheduler<K> triggerScheduler;

  // The trigger state corresponding to each {@link TriggerKey}.
  private final Map<TriggerKey<K>, TriggerImplHandler> triggers = new HashMap<>();

  public WindowOperatorImpl(WindowOperatorSpec<M, K, Object> windowOpSpec, Clock clock) {
    this.windowOpSpec = windowOpSpec;
    this.clock = clock;
    this.window = windowOpSpec.getWindow();
    this.triggerScheduler= new TriggerScheduler(clock);
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    WindowInternal<M, K, Object> window = windowOpSpec.getWindow();

    KeyValueStore<TimeSeriesKey<K>, Object> store = (KeyValueStore<TimeSeriesKey<K>, Object>) context.getStore(windowOpSpec.getOpName());

    // For windows that have a configured fold function, we use the store in over-write mode since we only retain the aggregated
    // value. Else, we use the store in append-mode.
    if (window.getFoldLeftFunction() != null) {
      window.getFoldLeftFunction().init(config, context);
      timeSeriesDb = new TimeSeriesStoreImpl(store, false);
    } else {
      timeSeriesDb = new TimeSeriesStoreImpl(store, true);
    }
  }

  private WindowKey<K> getWindowKey(M message) {
    Function<M, K> keyExtractor = window.getKeyExtractor();
    K key = (keyExtractor != null) ? keyExtractor.apply(message) : null ;

    String paneId = null;
    if (window.getWindowType() == WindowType.TUMBLING) {
      paneId = getTumblingWindowPaneId(message);
    } else if (window.getWindowType() == WindowType.SESSION) {
      paneId = getSessionWindowPaneId(message);
    }

    return new WindowKey<>(key, paneId);
  }

  /**
   * Computes the paneId of the window this message should belong to.
   *
   * In the case of tumbling windows, paneId of a window is defined as the start timestamp of its corresponding window
   * interval. For instance, if the tumbling interval is 10 seconds, all messages that arrive between [1000, 1010]
   * are assigned to the window with paneId "1000"
   *
   * @param message the input message
   * @return the paneId of the window this message should belong to
   */
  private String getTumblingWindowPaneId(M message) {
    long triggerDurationMs = ((TimeTrigger<M>) window.getDefaultTrigger()).getDuration().toMillis();
    final long now = clock.currentTimeMillis();
    long timestamp = now - now % triggerDurationMs;
    return Long.toString(timestamp);
  }

  /**
   * Computes the paneId of the window this message should belong to.
   *
   * In the case of session windows, paneId is defined as the timestamp of the earliest message in the window.
   * For instance, if the session gap is 10 seconds, and the first message in the window arrives at "1002" seconds,
   * all messages (that arrive within 10 seconds of their previous message) are assigned a paneId "1002".
   *
   * @param message the input message
   * @return the paneId of the window this message should belong to
   */

  private String getSessionWindowPaneId(M message) {
    K key = window.getKeyExtractor().apply(message);
    // get the value with the earliest timestamp for the provided key.
    List<TimestampedValue<Object>> timestampedValues = timeSeriesDb.get(key, 0, Long.MAX_VALUE, 1);

    // If there are no existing sessions for the key, we use the current timestamp as the paneId. If not, use the
    // timestamp of the earliest message as the paneId.
    long timestamp = (timestampedValues.isEmpty())? clock.currentTimeMillis() : timestampedValues.get(0).getTimestamp();

    return Long.toString(timestamp);
  }


  @Override
  public Collection<WindowPane<K, Object>> handleMessage(
      M message, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Processing message envelope: {}", message);
    List<WindowPane<K, Object>> results = new ArrayList<>();

    WindowKey<K> windowKey =  getWindowKey(message);
    long timestamp = Long.parseLong(windowKey.getPaneId());

    K key = windowKey.getKey();

    // For windows with a fold function configured (aka. aggregating windows), we only store the aggregated window value.
    // For windows with no fold function configured, we store all messages in the window.
    if (window.getFoldLeftFunction() == null) {
      timeSeriesDb.put(key, message, timestamp);
    } else {
      List<Object> existingState = getValues(windowKey);
      Preconditions.checkState(existingState.size() == 1, "Store with FoldLeftFunction should not contain more than one entry per window");
      Object aggregatedValue = applyFoldFunction(existingState.get(0), message);
      timeSeriesDb.put(key, aggregatedValue, timestamp);
    }

    if (window.getEarlyTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(FiringType.EARLY, windowKey);
      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getEarlyTrigger());
      Optional<WindowPane<K, Object>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(FiringType.DEFAULT, windowKey);
      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getDefaultTrigger());
      Optional<WindowPane<K, Object>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    return results;
  }

  private List<Object> getValues(WindowKey<K> windowKey) {
    long timestamp = Long.parseLong(windowKey.getPaneId());
    ClosableIterator<TimestampedValue<Object>> iterator = timeSeriesDb.get(windowKey.getKey(), timestamp);
    List<Object> values = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        TimestampedValue<Object> next = iterator.next();
        values.add(next.getValue());
      }
    } finally {
      iterator.close();
    }
    return values;
  }

  @Override
  public Collection<WindowPane<K, Object>> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Processing timer.");
    List<WindowPane<K, Object>> results = new ArrayList<>();

    List<TriggerKey<K>> keys = triggerScheduler.runPendingCallbacks();

    for (TriggerKey<K> key : keys) {
      TriggerImplHandler triggerImplHandler = triggers.get(key);
      if (triggerImplHandler != null) {
        Optional<WindowPane<K, Object>> maybeTriggeredPane = triggerImplHandler.onTimer(key, collector, coordinator);
        maybeTriggeredPane.ifPresent(results::add);
      }
    }
    LOG.trace("Triggered panes: " + results.size());
    return results;
  }

  @Override
  protected OperatorSpec<M, WindowPane<K, Object>> getOperatorSpec() {
    return windowOpSpec;
  }

  @Override
  protected void handleClose() {
    WindowInternal<M, K, Object> window = windowOpSpec.getWindow();

    if (window.getFoldLeftFunction() != null) {
      window.getFoldLeftFunction().close();
    }
  }

  private Object applyFoldFunction(Object oldVal, M message) {
    if (oldVal == null) {
      LOG.trace("No existing state found for key. Invoking initializer.");
      oldVal = window.getInitializer().get();
    }

    Object newVal = window.getFoldLeftFunction().apply(message, oldVal);
    return newVal;
  }

  private TriggerImplHandler getOrCreateTriggerImplHandler(TriggerKey<K> triggerKey, Trigger<M> trigger) {
    TriggerImplHandler wrapper = triggers.get(triggerKey);
    if (wrapper != null) {
      LOG.trace("Returning existing trigger wrapper for {}", triggerKey);
      return wrapper;
    }

    LOG.trace("Creating a new trigger wrapper for {}", triggerKey);

    TriggerImpl<M, K> triggerImpl = TriggerImpls.createTriggerImpl(trigger, clock, triggerKey);
    wrapper = new TriggerImplHandler(triggerKey, triggerImpl);
    triggers.put(triggerKey, wrapper);

    return wrapper;
  }

  /**
   * Handles trigger firings and returns the optional result.
   */
  private Optional<WindowPane<K, Object>> onTriggerFired(
          TriggerKey<K> triggerKey, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Trigger key {} fired." , triggerKey);

    TriggerImplHandler wrapper = triggers.get(triggerKey);
    WindowKey<K> windowKey = triggerKey.getKey();
    long timestamp = Long.parseLong(windowKey.getPaneId());
    List<Object> existingState = getValues(windowKey);

    if (existingState == null || existingState.size() == 0) {
      LOG.trace("No state found for triggerKey: {}", triggerKey);
      return Optional.empty();
    }
    // For windows that have a fold function configured return the aggregated value. Else, return the entire list of messages
    Object windowVal = window.getFoldLeftFunction() == null ? existingState : existingState.get(0);

    WindowPane<K, Object> paneOutput = computePaneOutput(triggerKey, windowVal);

    // Handle different accumulation modes.
    if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
      LOG.trace("Clearing state for trigger key: {}", triggerKey);
      timeSeriesDb.remove(windowKey.getKey(), timestamp);
    }

    // Cancel all early triggers too when the default trigger fires. Also, clean all state for the key.
    // note: We don't handle late arrivals yet, So, all arrivals are either early or on-time.
    if (triggerKey.getType() == FiringType.DEFAULT) {

      LOG.trace("Default trigger fired. Canceling triggers for {}", triggerKey);

      cancelTrigger(triggerKey, true);
      cancelTrigger(new TriggerKey(FiringType.EARLY, triggerKey.getKey()), true);
      timeSeriesDb.remove(windowKey.getKey(), timestamp);
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
  private WindowPane<K, Object> computePaneOutput(TriggerKey<K> triggerKey, Object windowVal) {
    WindowKey<K> windowKey = triggerKey.getKey();

    WindowPane<K, Object> paneOutput =
        new WindowPane<>(windowKey, windowVal, window.getAccumulationMode(), triggerKey.getType());
    LOG.trace("Emitting pane output for trigger key {}", triggerKey);
    return paneOutput;
  }

  /**
   * Cancels the firing of the {@link TriggerImpl} identified by this {@link TriggerKey} and optionally removes it.
   */
  private void cancelTrigger(TriggerKey<K> triggerKey, boolean shouldRemove) {
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
    private final TriggerImpl<M, K> impl;
    // Guard to ensure that we don't invoke onMessage or onTimer on already cancelled triggers
    private boolean isCancelled = false;

    public TriggerImplHandler(TriggerKey<K> key, TriggerImpl<M, K> impl) {
      this.impl = impl;
    }

    public Optional<WindowPane<K, Object>> onMessage(TriggerKey<K> triggerKey, M message,
                                                 MessageCollector collector, TaskCoordinator coordinator) {
      if (!isCancelled) {
        LOG.trace("Forwarding callbacks for {}", message);
        impl.onMessage(message, triggerScheduler);

        if (impl.shouldFire()) {
          // repeating trigger can trigger multiple times, So, clear the state to allow future triggerings.
          if (impl instanceof RepeatingTriggerImpl) {
            ((RepeatingTriggerImpl<M, K>) impl).clear();
          }
          return onTriggerFired(triggerKey, collector, coordinator);
        }
      }
      return Optional.empty();
    }

    public Optional<WindowPane<K, Object>> onTimer(
            TriggerKey<K> key, MessageCollector collector, TaskCoordinator coordinator) {
      if (impl.shouldFire() && !isCancelled) {
        LOG.trace("Triggering timer triggers");

        // repeating trigger can trigger multiple times, So, clear the trigger to allow future triggerings.
        if (impl instanceof RepeatingTriggerImpl) {
          ((RepeatingTriggerImpl<M, K>) impl).clear();
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