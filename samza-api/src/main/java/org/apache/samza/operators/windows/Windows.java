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

package org.apache.samza.operators.windows;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Factory methods for creating a {@link Window}. Provides APIs for specifying different types of {@link Window} transforms.
 *
 */
public class Windows {

  /**
   * Returns a keyed {@link Window} that windows groups its values per-key into fixed-size processing time-based windows
   * and aggregates values and applying the provided aggregator function. The windows are non-overlapping.
   *
   * <p>The below example groups the data by the provided key and into fixed-size 10 second windows for each key. It
   * emits the maximum element per-window per-key.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function< UserClick, String> keyExtractor = ...;
   *  BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowKey<String>, Integer>> windowed = integerStream.window(
   *  Windows.tumblingWindow(keyExtractor, Time.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param interval the duration in processing time on which the window is computed.
   * @param aggregateFunction the function to aggregate window results
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @param <K> type of the key in the {@link Window}
   * @return the created {@link Window} function.
   */


  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>>
    keyedTumblingWindow(Function<M, K> keyFn, Time interval, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();
    long intervalMs = interval.toMilliseconds();
    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.PeriodicTimeTrigger(TriggersBuilder.TriggerType.DEFAULT, intervalMs,
        Time.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new BaseWindow<M, K, WV>(keyFn, aggregateFunction, null, defaultTriggers);
  }


  /**
   * Returns a keyed {@link Window} that windows groups its values per-key into fixed-size processing time-based windows.
   * The windows are non-overlapping.
   *
   * <p>The below example groups the data by the provided key and into fixed-size 10 second tumbling windows for each key.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function< UserClick, String> keyExtractor = ...;
   *  MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowed = integerStream.window(
   *  Windows.tumblingWindow(keyExtractor, Time.seconds(10)));
   * }
   * </pre>
   *
   * @param interval the duration in processing time on which the window is computed.
   * @param keyFn function to extract key from the {@link MessageEnvelope}.
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link Window}
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>>
    keyedTumblingWindow(Function<M, K> keyFn, Time interval) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedTumblingWindow(keyFn, interval, aggregator);
  }

  /**
   * Returns a {@link Window} that windows values into fixed-size processing time-based windows and aggregates values
   * applying the provided aggregator function.
   *
   * <p>For example, in order to partition the data by 10 second windows and emit the maximum element every 10 seconds.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   *  BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowInfo, Integer>> windowed = integerStream.window(
   *  Windows.tumblingWindow(Time.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param duration the duration in processing time on which the window is computed.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>>
    tumblingWindow(Time duration, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.PeriodicTimeTrigger(TriggersBuilder.TriggerType.DEFAULT, duration.toMilliseconds(),
        Time.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new BaseWindow<M, Void, WV>(null, aggregateFunction, null, defaultTriggers);
  }

  /**
   * Returns a {@link Window} that windows its values into fixed-size processing time-based windows.
   *
   * <p>For example, in order to partition the data by 10 minute windows and apply a map transform later on the list of
   * messages in the window.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long, Long>> percentile99 = ..
   *
   *  MessageStream<WindowOutput<WindowInfo, Collection<Long>>> windowed = integerStream.window(Windows.tumblingWindow(Time.minutes(10));
   *  MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage())));
   * }
   * </pre>
   *
   * @param duration the duration in processing time on which the window is computed.
   * @param <M> type of the input {@link MessageEnvelope}
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope> Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> tumblingWindow(Time duration) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return tumblingWindow(duration, aggregator);
  }

  /**
   * Returns a {@link Window} that windows values into sessions based on the {@code sessionGap} and aggregates values
   * applying the provided aggregator function. A Session captures some period of activity over a {@code MessageStream}.
   * The boundary for the session is defined by a timeout gap. All data that arrives within a span of the timeout gap are
   * grouped to constitute a single session.
   *
   * <p>For example, in order to group the data by 10 second session windows and emit the maximum element in each session.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   *  BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowInfo, Integer>> windowed = integerStream.window(
   *  Windows.sessionWindow(Time.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> sessionWindow(Time sessionGap, BiFunction<M, WV, WV> aggregateFunction) {

    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.TimeSinceLastMessageTrigger(TriggersBuilder.TriggerType.DEFAULT, sessionGap.toMilliseconds(),
      Time.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new BaseWindow<M, Void, WV>(null, aggregateFunction, null, defaultTriggers);
  }

  /**
   * Returns a {@link Window} that windows values into sessions based on the provided {@code sessionGap}. A Session
   * captures some period of activity over a {@code MessageStream}.
   *
   * The boundary for the session is defined by a timeout gap. All data that arrives within a span of the timeout gap
   * are grouped to constitute a single session.
   *
   * <p>For example, in order to partition the data by 10 minute session windows and apply a map transform later on the list of
   * messages in the window.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long, Long>> percentile99 = ..
   *
   *  MessageStream<WindowOutput<WindowInfo, Collection<Long>>> windowed = integerStream.window(Windows.sessionWindows(Time.minutes(10));
   *  MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage())));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> type of the input {@link MessageEnvelope}
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope> Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> sessionWindow(Time sessionGap) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return sessionWindow(sessionGap, aggregator);
  }

  /**
   * Returns a keyed {@link Window} that windows values into sessions based on the {@code sessionGap}
   * and aggregates values applying the provided aggregator function. Windowing operations are done "per-key".
   *
   * <p>A Session captures some period of activity over a {@code MessageStream}. The boundary for the session is defined by
   * the timeout gap per-key. All data that arrives within a span of the timeout gap are grouped into a
   * single session for that key.
   *
   * <p>For example, to track sessions(defined by a timeout gap of 1 minute) grouped by userId and compute some
   * aggregation over a stream of user clicks.
   *
   * <pre> {@code
   * MessageStream<UserClicks> stream = ...;
   *  BiFunction<UserClicks, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  Function<UserClicks, String> userIdExtractor = m -> m.getUserId()..;
   *  MessageStream<WindowOutput<WindowKey, Integer>> windowed = integerStream.window(
   *  Windows.keyedSessionWindow(userIdExtractor, Time.minute(1), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link Window}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link Window} function.
   */


  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedSessionWindow(Function<M, K> keyFn, Time sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    List<TriggersBuilder.Trigger> defaultTriggers = new ArrayList<>();

    TriggersBuilder.Trigger timeTrigger = new TriggersBuilder.TimeSinceLastMessageTrigger(TriggersBuilder.TriggerType.DEFAULT, sessionGap.toMilliseconds(),
      Time.TimeCharacteristic.PROCESSING_TIME);
    defaultTriggers.add(timeTrigger);

    return new BaseWindow<M, K, WV>(keyFn, aggregateFunction, null, defaultTriggers);
  }


  /**
   * Returns a keyed {@link Window} that windows values into sessions based on the {@code sessionGap}
   * Windowing operations are done "per-key".
   *
   * <p>A Session captures some period of activity over a {@code MessageStream}. The boundary for the session is defined by
   * the timeout gap per-key. All data that arrives within a span of the timeout gap are grouped into a
   * single session for that key.
   *
   * <p>For example, to track sessions(defined by a timeout gap of 1 minute)  grouped by userId.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   *  BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseIntField(m), c);
   *  Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *  MessageStream<WindowOutput<WindowKey<String>, Collection<M>>> windowed = stream.window(
   *  Windows.keyedSessionWindow(userIdExtractor, Time.minute(1)));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link Window}
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedSessionWindow(Function<M, K> keyFn, Time sessionGap) {

    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedSessionWindow(keyFn, sessionGap, aggregator);
  }


  /**
   * Returns a global {@link Window} that windows all values into a single large window. This window does not have a
   * default triggering behavior. The triggering behavior must be specified by setting early triggers using the APIs
   * of {@link TriggersBuilder}.
   *
   * <p>For example, to window a stream into count based windows that trigger every 50 messages or every 10 minutes and
   * compute the maximum value over each window.

   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * BiFunction<Long, Long, Long> maxAggregator = (m, c)-> Math.max(m, c);
   * MessageStream<WindowOutput<WindowKey, Long>> windowed = stream.window(Windows.globalWindow(maxAggregator)
   * .setTriggers(new TriggersBuilder()
   *   .withEarlyFiringsEvery(Time.minutes(10))
   *   .withEarlyFiringsAfterCountAtleast(50).build()));
   * }
   * </pre>
   *
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link Window} function.
   */
  public static <M extends MessageEnvelope, WV> Window<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> globalWindow(BiFunction<M, WV, WV> aggregateFunction) {
    return new BaseWindow<M, Void, WV>(null, aggregateFunction, null, null);
  }

  /**
   * Returns a global {@link Window} that windows all values into a single large window. This window does not have a
   * default triggering behavior. The triggering behavior must be specified by setting early triggers using the APIs
   * of {@link TriggersBuilder}.
   *
   * For example, to window a stream into count based windows that trigger every 50 messages or every 10 minutes.
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowed = stream.window(Windows.globalWindow()
   * .setTriggers(new TriggersBuilder()
   *   .withEarlyFiringsEvery(Time.minutes(10))
   *   .withEarlyFiringsAfterCountAtleast(50).build()));
   * }
   * </pre>
   *
   * @param <M> the type of {@link MessageEnvelope}
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope> Window<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> globalWindow() {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return globalWindow(aggregator);
  }

  /**
   * Returns a global {@link Window} that groups the values per-key and assigns them to their own global window.
   * This window does not have a default triggering behavior. The triggering behavior must be specified by setting early
   * triggers using the APIs of {@link TriggersBuilder}. Triggers are fired per-key.
   *
   * <p> The below example groups the stream by the key and windows the stream into count based windows. The window triggers
   * every 50 messages or every 10 minutes. It emits the maximum value per-window per-key.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * BiFunction<UserClick, Long, Long> maxAggregator = (m, c)-> Math.max(parseLongField(m), c);
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Long>> windowed = stream.window(Windows.keyedGlobalWindow(keyFn, maxAggregator)
   * .setTriggers(new TriggersBuilder()
   *   .withEarlyFiringsEvery(Time.minutes(10))
   *   .withEarlyFiringsAfterCountAtleast(50).build()));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> type of the key in the {@link Window}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link Window} function.
   */

  public static <M extends MessageEnvelope, K, WV> Window<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedGlobalWindow(Function<M, K> keyFn, BiFunction<M, WV, WV> aggregateFunction) {
    return new BaseWindow<M, K, WV>(keyFn, aggregateFunction, null, null);
  }

  /**
   * Returns a global {@link Window} that groups the values per-key and assigns them to their own global window.
   * This window does not have a default triggering behavior. The triggering behavior must be specified by setting early
   * triggers using the APIs of {@link TriggersBuilder}. Triggers are fired per-key.
   *
   * <p> The below example groups the stream by the key and windows the stream into count based windows. The window triggers
   * every 50 messages or every 10 minutes.

   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowed = stream.window(Windows.keyedGlobalWindow(keyFn)
   * .setTriggers(new TriggersBuilder()
   *   .withEarlyFiringsEvery(Time.minutes(10))
   *   .withEarlyFiringsAfterCountAtleast(50).build()));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> type of the key in the {@link Window}
   * @return the created {@link Window} function.
   */


  public static <M extends MessageEnvelope, K> Window<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedGlobalWindow(Function<M, K> keyFn) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedGlobalWindow(keyFn, aggregator);
  }
}
