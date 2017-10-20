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
import org.apache.samza.operators.functions.FoldLeftFunction;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of a window operator that groups messages into finite windows for processing.
 *
 * This class implements the processing logic for various types of windows and triggers. It tracks and manages state for
 * all open windows, the active triggers that correspond to each of the windows and the pending callbacks. It provides
 * an implementation of {@link TriggerScheduler} that {@link TriggerImpl}s can use
 * to schedule and cancel callbacks. It also orchestrates the flow of messages through the various
 * {@link TriggerImpl}s.
 *
 * <p> An instance of a {@link TriggerImplHandler} is created corresponding to each {@link Trigger}
 * configured for a particular window. For every message added to the window, this class looks up the corresponding
 * {@link TriggerImplHandler} for the trigger and invokes {@link TriggerImplHandler#onMessage(TriggerKey, Object,
 * MessageCollector, TaskCoordinator)} The {@link TriggerImplHandler} maintains the {@link TriggerImpl} instance along
 * with whether it has been canceled yet or not. Then, the {@link TriggerImplHandler} invokes onMessage on underlying
 * its {@link TriggerImpl} instance. A {@link TriggerImpl} instance is scoped to a window and its firing determines when
 * results for its window are emitted.
 *
 * The {@link WindowOperatorImpl} checks if the trigger fired and returns the result of the firing.
 *
 * @param <M> the type of the incoming message
 * @param <K> the type of the key in the incoming message
 *
 */
public class WindowOperatorImpl<M, K> extends OperatorImpl<M, WindowPane<K, Object>> {
  // Object == Collection<M> || WV
  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorImpl.class);

  private final WindowOperatorSpec<M, K, Object> windowOpSpec;
  private final Clock clock;
  private final WindowInternal<M, K, Object> window;
  private final FoldLeftFunction<M, Object> foldLeftFn;
  private final Supplier<Object> initializer;
  private final Function<M, K> keyFn;

  private final TriggerScheduler<K> triggerScheduler;
  private final Map<TriggerKey<K>, TriggerImplHandler> triggers = new HashMap<>();
  private TimeSeriesStore<K, Object> timeSeriesStore;

  public WindowOperatorImpl(WindowOperatorSpec<M, K, Object> windowOpSpec, Clock clock) {
    this.windowOpSpec = windowOpSpec;
    this.clock = clock;
    this.window = windowOpSpec.getWindow();
    this.foldLeftFn = window.getFoldLeftFunction();
    this.initializer = window.getInitializer();
    this.keyFn = window.getKeyExtractor();
    this.triggerScheduler= new TriggerScheduler(clock);
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    WindowInternal<M, K, Object> window = windowOpSpec.getWindow();

    KeyValueStore<TimeSeriesKey<K>, Object> store =
        (KeyValueStore<TimeSeriesKey<K>, Object>) context.getStore(windowOpSpec.getOpId());

    // For aggregating windows, we use the store in over-write mode since we only retain the aggregated
    // value. Else, we use the store in append-mode.
    if (foldLeftFn != null) {
      foldLeftFn.init(config, context);
      timeSeriesStore = new TimeSeriesStoreImpl(store, false);
    } else {
      timeSeriesStore = new TimeSeriesStoreImpl(store, true);
    }
  }

  @Override
  public Collection<WindowPane<K, Object>> handleMessage(
      M message, MessageCollector collector, TaskCoordinator coordinator) {
    LOG.trace("Processing message envelope: {}", message);
    List<WindowPane<K, Object>> results = new ArrayList<>();

    K key = (keyFn != null) ? keyFn.apply(message) : null;
    long timestamp = getWindowTimestamp(message);

    // For aggregating windows, we only store the aggregated window value.
    // For non-aggregating windows, we store all messages in the window.
    if (foldLeftFn == null) {
      timeSeriesStore.put(key, message, timestamp); // store is in append mode
    } else {
      List<Object> existingState = getValues(key, timestamp);
      Preconditions.checkState(existingState.size() == 1, "WindowState for aggregating windows " +
          "must not contain more than one entry per window");

      Object oldVal = existingState.get(0);
      if (oldVal == null) {
        LOG.trace("No existing state found for key. Invoking initializer.");
        oldVal = initializer.get();
      }

      Object aggregatedValue = foldLeftFn.apply(message, oldVal);
      timeSeriesStore.put(key, aggregatedValue, timestamp);
    }

    if (window.getEarlyTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(FiringType.EARLY, key, timestamp);
      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getEarlyTrigger());
      Optional<WindowPane<K, Object>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    if (window.getDefaultTrigger() != null) {
      TriggerKey<K> triggerKey = new TriggerKey<>(FiringType.DEFAULT, key, timestamp);
      TriggerImplHandler triggerImplHandler = getOrCreateTriggerImplHandler(triggerKey, window.getDefaultTrigger());
      Optional<WindowPane<K, Object>> maybeTriggeredPane =
          triggerImplHandler.onMessage(triggerKey, message, collector, coordinator);
      maybeTriggeredPane.ifPresent(results::add);
    }

    return results;
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
    if (foldLeftFn != null) {
      foldLeftFn.close();
    }
    if (timeSeriesStore != null) {
      timeSeriesStore.close();
    }
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
  private Optional<WindowPane<K, Object>> onTriggerFired(TriggerKey<K> triggerKey, MessageCollector collector,
      TaskCoordinator coordinator) {
    LOG.trace("Trigger key {} fired." , triggerKey);

    TriggerImplHandler wrapper = triggers.get(triggerKey);
    long timestamp = triggerKey.getTimestamp();
    K key = triggerKey.getKey();
    List<Object> existingState = getValues(key, timestamp);

    if (existingState == null || existingState.size() == 0) {
      LOG.trace("No state found for triggerKey: {}", triggerKey);
      return Optional.empty();
    }

    Object windowVal = window.getFoldLeftFunction() == null ? existingState : existingState.get(0);

    WindowPane<K, Object> paneOutput = computePaneOutput(triggerKey, windowVal);

    // Handle different accumulation modes.
    if (window.getAccumulationMode() == AccumulationMode.DISCARDING) {
      LOG.trace("Clearing state for trigger key: {}", triggerKey);
      timeSeriesStore.remove(key, timestamp);
    }

    // Cancel all early triggers too when the default trigger fires. Also, clean all state for the key.
    // note: We don't handle late arrivals yet, So, all arrivals are either early or on-time.
    if (triggerKey.getType() == FiringType.DEFAULT) {

      LOG.trace("Default trigger fired. Canceling triggers for {}", triggerKey);

      cancelTrigger(triggerKey, true);
      cancelTrigger(new TriggerKey(FiringType.EARLY, triggerKey.getKey(), triggerKey.getTimestamp()), true);
      timeSeriesStore.remove(key, timestamp);
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
    WindowKey<K> windowKey = new WindowKey(triggerKey.getKey(), Long.toString(triggerKey.getTimestamp()));
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
   * Computes the timestamp of the window this message should belong to.
   *
   * In the case of tumbling windows, timestamp of a window is defined as the start timestamp of its corresponding window
   * interval. For instance, if the tumbling interval is 10 seconds, all messages that arrive between [1000, 1010]
   * are assigned to the window with timestamp "1000"
   *
   * In the case of session windows, timestamp is defined as the timestamp of the earliest message in the window.
   * For instance, if the session gap is 10 seconds, and the first message in the window arrives at "1002" seconds,
   * all messages (that arrive within 10 seconds of their previous message) are assigned a timestamp "1002".
   *
   * @param message the input message
   * @return the timestamp of the window this message should belong to
   */
  private long getWindowTimestamp(M message) {
    if (window.getWindowType() == WindowType.TUMBLING) {
      long triggerDurationMs = ((TimeTrigger<M>) window.getDefaultTrigger()).getDuration().toMillis();
      final long now = clock.currentTimeMillis();
      // assign timestamp to be the start timestamp of the window boundary
      long timestamp = now - now % triggerDurationMs;
      return timestamp;
    } else {
      K key = keyFn.apply(message);
      // get the value with the earliest timestamp for the provided key.
      ClosableIterator<TimestampedValue<Object>> iterator = timeSeriesStore.get(key, 0, Long.MAX_VALUE, 1);
      List<TimestampedValue<Object>> timestampedValues = toList(iterator);

      // If there are no existing sessions for the key, we return the current timestamp. If not, return the
      // timestamp of the earliest message.
      long timestamp = (timestampedValues.isEmpty())? clock.currentTimeMillis() : timestampedValues.get(0).getTimestamp();

      return timestamp;
    }
  }

  /**
   * Return a list of values in the store for the provided key and timestamp
   *
   * @param key the key to look up in the store
   * @param timestamp the timestamp to look up in the store
   * @return the list of values for the provided key
   */
  private List<Object> getValues(K key, long timestamp) {
    ClosableIterator<TimestampedValue<Object>> iterator = timeSeriesStore.get(key, timestamp);
    List<TimestampedValue<Object>> timestampedValues = toList(iterator);
    List<Object> values = timestampedValues.stream().map(element -> element.getValue()).collect(Collectors.toList());
    return values;
  }

  /**
   * Returns an unmodifiable list of all elements in the provided iterator.
   * The iterator is guaranteed to be closed after its execution.
   *
   * @param iterator the provided iterator.
   * @param <V> the type of elements in the iterator
   * @return a list of all elements returned by the iterator
   */
  static <V>  List<V> toList(ClosableIterator<V> iterator) {
    List<V> values = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        values.add(iterator.next());
      }
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
    return Collections.unmodifiableList(values);
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

    public Optional<WindowPane<K, Object>> onTimer(TriggerKey<K> key, MessageCollector collector,
        TaskCoordinator coordinator) {
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