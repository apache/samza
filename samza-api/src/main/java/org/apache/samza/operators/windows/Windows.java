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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.triggers.Duration;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Factory methods for creating a {@link WindowFunction}. Provides APIs for specifying different types of {@link WindowFunction}s.
 *
 */
@InterfaceStability.Unstable
public final class Windows {

  /**
   * Returns a {@link WindowFunction} that windows its values per-key into fixed-size processing time based windows
   * and aggregates them applying the provided function. The windows are non-overlapping.
   *
   * <p>The below example computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyExtractor = ...;
   *  BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowKey<String>, Integer>> windowed = stream.window(
   *  Windows.keyedTumblingWindow(keyExtractor, Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param interval the duration in processing time on which the window is computed.
   * @param aggregateFunction the function to aggregate window results
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @param <K> type of the key in the {@link WindowFunction}
   * @return the created {@link WindowFunction} function.
   */


  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>>
    keyedTumblingWindow(Function<M, K> keyFn, Duration interval, BiFunction<M, WV, WV> aggregateFunction) {

    Trigger defaultTrigger = new TimeTrigger(interval);
    return new BaseWindowFunction<M, K, WV>(defaultTrigger, aggregateFunction, keyFn, null);
  }


  /**
   * Returns a {@link WindowFunction} that windows its values per-key into fixed-size processing time based windows.
   * The windows are non-overlapping.
   *
   * <p>The below example windows the stream into fixed-size 10 second windows for each key.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyExtractor = ...;
   *  MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowed = stream.window(
   *  Windows.keyedTumblingWindow(keyExtractor, Duration.seconds(10)));
   * }
   * </pre>
   *
   * @param interval the duration in processing time on which the window is computed.
   * @param keyFn function to extract key from the {@link MessageEnvelope}.
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope, K> WindowFunction<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>>
    keyedTumblingWindow(Function<M, K> keyFn, Duration interval) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedTumblingWindow(keyFn, interval, aggregator);
  }

  /**
   * Returns a {@link WindowFunction} that windows values into fixed-size processing time based windows and aggregates
   * them applying the provided function.
   *
   * <p>The below example computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   *  BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowKey, Integer>> windowed = stream.window(
   *  Windows.tumblingWindow(Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param duration the duration in processing time on which the window is computed.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope, WV> WindowFunction<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>>
    tumblingWindow(Duration duration, BiFunction<M, WV, WV> aggregateFunction) {

    Trigger defaultTrigger = Triggers.repeat(new TimeTrigger(duration));

    return new BaseWindowFunction<M, Void, WV>(defaultTrigger, aggregateFunction, null, null);
  }

  /**
   * Returns a {@link WindowFunction} that windows its values into fixed-size processing time based windows.
   *
   * <p>The below example windows the stream into fixed-size 10 second windows, and computes a windowed-percentile.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long, Long>> percentile99 = ..
   *
   *  MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowed = integerStream.window(Windows.tumblingWindow(Duration.seconds(10)));
   *  MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage());
   * }
   * </pre>
   *
   * @param duration the duration in processing time on which the window is computed.
   * @param <M> type of the input {@link MessageEnvelope}
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope> WindowFunction<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> tumblingWindow(Duration duration) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return tumblingWindow(duration, aggregator);
  }

  /**
   * Returns a {@link WindowFunction} that windows values into sessions based on the {@code sessionGap} and aggregates them
   * applying the provided function. A <i>session</i> captures some period of activity over a {@link MessageStream}.
   * The boundary for the session is defined by a {@code sessionGap}. All data that arrives within the gap are
   * grouped into the same session.
   *
   * <p>The below example computes the maximum value over a session window of gap 10 seconds.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   *  BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  MessageStream<WindowOutput<WindowKey, Integer>> windowed = stream.window(
   *  Windows.sessionWindow(Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope, WV> WindowFunction<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> sessionWindow(Duration sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    Trigger defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);

    return new BaseWindowFunction<M, Void, WV>(defaultTrigger, aggregateFunction, null, null);
  }

  /**
   * Returns a {@link WindowFunction} that windows values into sessions based on the provided {@code sessionGap}.  A
   * session captures some period of activity over a {@link MessageStream}. The boundary for the session is defined by the
   * {@code sessionGap}. All data that arrives within the gap are grouped into the same session.
   *
   * <p>The below example windows the stream into session windows with gap 10 seconds, and computes a windowed-percentile.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long>, Long>> percentile99 = ..
   *
   *  MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowed = integerStream.window(Windows.sessionWindows(Duration.minutes(10));
   *  MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage())));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> type of the input {@link MessageEnvelope}
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope> WindowFunction<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> sessionWindow(Duration sessionGap) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return sessionWindow(sessionGap, aggregator);
  }

  /**
   * Returns a {@link WindowFunction} that windows values into sessions per-key based on the provided {@code sessionGap} and
   * aggregates them applying the provided function. A session captures some period of activity over a {@link MessageStream}.
   * The boundary for the session is defined by the {@code sessionGap}. All data that arrives within the gap are grouped
   * into the same session.
   *
   * <p>The below example computes the maximum value per-key over a session window of gap 10 seconds.

   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   *  BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *  Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *  MessageStream<WindowOutput<WindowKey<String>, Integer>> windowed = stream.window(
   *  Windows.keyedSessionWindow(userIdExtractor, Duration.minute(1), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedSessionWindow(Function<M, K> keyFn, Duration sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    Trigger defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);

    return new BaseWindowFunction<M, K, WV>(defaultTrigger, aggregateFunction, keyFn, null);
  }


  /**
   * Returns a {@link WindowFunction} that windows values into sessions per-key based on the provided {@code sessionGap}.
   * A session captures some period of activity over a {@link MessageStream}. The boundary for the session is defined by
   * the {@code sessionGap}. All data that arrives within the gap are grouped into the same session.
   *
   * <p>The below example windows the stream into per-key session windows of gap 10 seconds.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   *  BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseIntField(m), c);
   *  Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *  MessageStream<WindowOutput<WindowKey<String>, Collection<M>>> windowed = stream.window(
   *  Windows.keyedSessionWindow(userIdExtractor, Duration.seconds(10)));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> type of the input {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope, K> WindowFunction<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedSessionWindow(Function<M, K> keyFn, Duration sessionGap) {

    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedSessionWindow(keyFn, sessionGap, aggregator);
  }


  /**
   * Returns a {@link WindowFunction} that windows all values into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting early triggers using the APIs
   * of {@link org.apache.samza.operators.triggers.Triggers}.

   * <p>The below example windows computes the maximum value over a count based window. The window emits results when
   * there are either 50 messages in the window or when 10 seconds have passed since the first message.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * BiFunction<Long, Long, Long> maxAggregator = (m, c)-> Math.max(m, c);
   * MessageStream<WindowOutput<WindowKey, Long>> windowed = stream.window(Windows.globalWindow(maxAggregator)
   * .setEarlyTriggers(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.seconds(10))))))
   * }
   * </pre>
   *
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of {@link MessageEnvelope}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */
  public static <M extends MessageEnvelope, WV> WindowFunction<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> globalWindow(BiFunction<M, WV, WV> aggregateFunction) {
    return new BaseWindowFunction<M, Void, WV>(null, aggregateFunction, null, null);
  }

  /**
   * Returns a {@link WindowFunction} that windows all values into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting early triggers using
   * {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * For example, to window a stream into count based windows that trigger every 50 messages or every 10 minutes.
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowed = stream.window(Windows.globalWindow()
   * .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.seconds(10))))))
   * }
   * </pre>
   *
   * @param <M> the type of {@link MessageEnvelope}
   * @return the created {@link WindowFunction} function.
   */

  public static <M extends MessageEnvelope> WindowFunction<M, Void, WindowKey<Void>, Collection<M>, WindowOutput<WindowKey<Void>, Collection<M>>> globalWindow() {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return globalWindow(aggregator);
  }

  /**
   * Returns a global {@link WindowFunction} that windows its values per-key. The window does not have a default trigger.
   * The triggering behavior must be specified by setting early
   * triggers using the {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * <p> The below example windows the stream into count based windows. The window triggers every 50 messages or every
   * 10 minutes.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * BiFunction<UserClick, Long, Long> maxAggregator = (m, c)-> Math.max(parseLongField(m), c);
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Long>> windowed = stream.window(Windows.keyedGlobalWindow(keyFn, maxAggregator)
   * .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @param <WV> type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function
   */

  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedGlobalWindow(Function<M, K> keyFn, BiFunction<M, WV, WV> aggregateFunction) {
    return new BaseWindowFunction<M, K, WV>(null, aggregateFunction, keyFn, null);
  }

  /**
   * Returns a global {@link WindowFunction} that windows its values per-key. The window does not have a default trigger.
   * The triggering behavior must be specified by setting early triggers using the {@link org.apache.samza.operators.triggers.Triggers}
   * APIs.
   *
   * <p> The below example windows the stream per-key into count based windows. The window triggers every 50 messages or
   * every 10 minutes.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowed = stream.window(Windows.keyedGlobalWindow(keyFn)
   * .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @return the created {@link WindowFunction} function
   */

  public static <M extends MessageEnvelope, K> WindowFunction<M, K, WindowKey<K>, Collection<M>, WindowOutput<WindowKey<K>, Collection<M>>> keyedGlobalWindow(Function<M, K> keyFn) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedGlobalWindow(keyFn, aggregator);
  }
}
