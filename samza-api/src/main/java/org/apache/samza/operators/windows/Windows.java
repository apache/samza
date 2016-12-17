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

  private Windows() {

  }

  /**
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s using the provided keyFn into
   * fixed-size, non-overlapping processing time based windows and applies the provided aggregate function to them.
   *
   * <p>The example below computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyFn = ...;
   * BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   * MessageStream<WindowOutput<WindowKey<String>, Integer>> windowedStream = stream.window(
   * Windows.keyedTumblingWindow(keyFn, Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param interval the duration in processing time.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <WV> the type of the {@link WindowOutput} output value
   * @param <K> the type of the key in the {@link WindowFunction}
   * @return the created {@link WindowFunction} function.
   */
  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>>
    keyedTumblingWindow(Function<M, K> keyFn, Duration interval, BiFunction<M, WV, WV> aggregateFunction) {

    Trigger defaultTrigger = new TimeTrigger(interval);
    return new BaseWindowFunction<M, K, WV>(defaultTrigger, aggregateFunction, keyFn, null);
  }


  /**
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s using the provided keyFn into
   * fixed-size, non-overlapping processing time based windows.
   *
   * <p>The example below windows the stream into fixed-size 10 second windows for each key.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowedStream = stream.window(
   * Windows.keyedTumblingWindow(keyFn, Duration.seconds(10)));
   * }
   * </pre>
   *
   * @param keyFn function to extract key from the {@link MessageEnvelope}.
   * @param interval the duration in processing time.
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <K> the type of the key in the {@link WindowFunction}
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
   * Creates a {@link WindowFunction} that windows values into fixed-size processing time based windows and aggregates
   * them applying the provided function.
   *
   * <p>The example below computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   * BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   * MessageStream<WindowOutput<WindowKey, Integer>> windowedStream = stream.window(
   * Windows.tumblingWindow(Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param duration the duration in processing time.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <WV> the type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */
  public static <M extends MessageEnvelope, WV> WindowFunction<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>>
    tumblingWindow(Duration duration, BiFunction<M, WV, WV> aggregateFunction) {

    Trigger defaultTrigger = Triggers.repeat(new TimeTrigger(duration));

    return new BaseWindowFunction<M, Void, WV>(defaultTrigger, aggregateFunction, null, null);
  }

  /**
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into fixed-size, non-overlapping
   * processing time based windows.
   *
   * <p>The example below windows the stream into fixed-size 10 second windows, and computes a windowed-percentile.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long, Long>> percentile99 = ..
   *
   * MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowedStream = integerStream.window(Windows.tumblingWindow(Duration.seconds(10)));
   * MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage());
   * }
   * </pre>
   *
   * @param duration the duration in processing time.
   * @param <M> the type of the input {@link MessageEnvelope}
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
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into sessions based on the
   * {@code sessionGap} and applies the provided function to them.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
   * A session is considered complete when no new messages arrive within the {@code sessionGap}. All
   * {@link MessageEnvelope}s that arrive within the gap are grouped into the same session.
   *
   * <p>The example below computes the maximum value over a session window of gap 10 seconds.
   *
   * <pre> {@code
   * MessageStream<String> stream = ...;
   * BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   * MessageStream<WindowOutput<WindowKey, Integer>> windowedStream = stream.window(
   * Windows.sessionWindow(Duration.seconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <WV> the type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */
  public static <M extends MessageEnvelope, WV> WindowFunction<M, Void, WindowKey<Void>, WV, WindowOutput<WindowKey<Void>, WV>> sessionWindow(Duration sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    Trigger defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);

    return new BaseWindowFunction<M, Void, WV>(defaultTrigger, aggregateFunction, null, null);
  }

  /**
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into sessions based on the {@code sessionGap}.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
   * A session is considered complete when no new messages arrive within the {@code sessionGap}. All {@link MessageEnvelope}s
   * that arrive within the gap are grouped into the same session.
   *
   * <p>The example below windows the stream into session windows with gap 10 seconds, and computes a windowed-percentile.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * Function<Collection<Long>, Long>> percentile99 = ..
   *
   * MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowedStream = integerStream.window(Windows.sessionWindows(Duration.minutes(10));
   * MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage())));
   * }
   * </pre>
   *
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> the type of the input {@link MessageEnvelope}
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
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into sessions per-key based on the provided {@code sessionGap}
   * and applies the provided aggregate function to them.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
   * A session is considered complete when no new messages arrive within the {@code sessionGap}. All {@link MessageEnvelope}s that arrive within
   * the gap are grouped into the same session.
   *
   * <p>The example below computes the maximum value per-key over a session window of gap 10 seconds.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   * Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   * MessageStream<WindowOutput<WindowKey<String>, Integer>> windowedStream = stream.window(
   * Windows.keyedSessionWindow(userIdExtractor, Duration.minute(1), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <K> the type of the key in the {@link WindowFunction}
   * @param <WV> the type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function.
   */
  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedSessionWindow(Function<M, K> keyFn, Duration sessionGap, BiFunction<M, WV, WV> aggregateFunction) {
    Trigger defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);

    return new BaseWindowFunction<M, K, WV>(defaultTrigger, aggregateFunction, keyFn, null);
  }


  /**
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into sessions per-key based on the provided {@code sessionGap}.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}. The
   * boundary for the session is defined by a {@code sessionGap}. All {@link MessageEnvelope}s that that arrive within
   * the gap are grouped into the same session.
   *
   * <p>The example below groups the stream into per-key session windows of gap 10 seconds.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseIntField(m), c);
   * Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   * MessageStream<WindowOutput<WindowKey<String>, Collection<M>>> windowedStream = stream.window(
   * Windows.keyedSessionWindow(userIdExtractor, Duration.seconds(10)));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param sessionGap the timeout gap for defining the session.
   * @param <M> the type of the input {@link MessageEnvelope}
   * @param <K> the type of the key in the {@link WindowFunction}
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
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting early triggers using the
   * {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * <p>The example below computes the maximum value over a count based window. The window emits results when
   * there are either 50 messages in the window or when 10 seconds have passed since the first message.
   *
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * BiFunction<Long, Long, Long> maxAggregator = (m, c)-> Math.max(m, c);
   * MessageStream<WindowOutput<WindowKey, Long>> windowedStream = stream.window(Windows.globalWindow(maxAggregator)
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
   * Creates a {@link WindowFunction} that groups incoming {@link MessageEnvelope}s into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting early triggers using the
   * {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * The example below groups a stream into count based windows that trigger every 50 messages or every 10 minutes.
   * <pre> {@code
   * MessageStream<Long> stream = ...;
   * MessageStream<WindowOutput<WindowKey, Collection<Long>>> windowedStream = stream.window(Windows.globalWindow()
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
   * Returns a global {@link WindowFunction} that groups incoming {@link MessageEnvelope}s using the provided keyFn.
   * The window does not have a default trigger. The triggering behavior must be specified by setting early
   * triggers using the {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * <p> The example below groups the stream into count based windows. The window triggers every 50 messages or every
   * 10 minutes.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * BiFunction<UserClick, Long, Long> maxAggregator = (m, c)-> Math.max(parseLongField(m), c);
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Long>> windowedStream = stream.window(Windows.keyedGlobalWindow(keyFn, maxAggregator)
   * .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param aggregateFunction the function to aggregate window results
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> type of the key in the {@link WindowFunction}
   * @param <WV> the type of the {@link WindowOutput} output value
   * @return the created {@link WindowFunction} function
   */
  public static <M extends MessageEnvelope, K, WV> WindowFunction<M, K, WindowKey<K>, WV, WindowOutput<WindowKey<K>, WV>> keyedGlobalWindow(Function<M, K> keyFn, BiFunction<M, WV, WV> aggregateFunction) {
    return new BaseWindowFunction<M, K, WV>(null, aggregateFunction, keyFn, null);
  }

  /**
   * Returns a global {@link WindowFunction} that groups incoming {@link MessageEnvelope}s using the provided keyFn.
   * The window does not have a default trigger. The triggering behavior must be specified by setting early triggers
   * using the {@link org.apache.samza.operators.triggers.Triggers} APIs.
   *
   * <p> The example below groups the stream per-key into count based windows. The window triggers every 50 messages or
   * every 10 minutes.
   *
   * <pre> {@code
   * MessageStream<UserClick> stream = ...;
   * Function<UserClick, String> keyFn = ...;
   * MessageStream<WindowOutput<WindowKey<String>, Collection<UserClick>>> windowedStream = stream.window(Windows.keyedGlobalWindow(keyFn)
   * .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the key from a {@link MessageEnvelope}
   * @param <M> the type of {@link MessageEnvelope}
   * @param <K> the type of the key in the {@link WindowFunction}
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
