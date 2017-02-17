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
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.internal.WindowInternal;

import java.time.Duration;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * APIs for creating different types of {@link Window}s.
 *
 * Groups the incoming messages in the {@link org.apache.samza.operators.MessageStream} into finite windows for processing.
 *
 * <p> Each window is uniquely identified by its {@link WindowKey}. A window can have one or more associated {@link Trigger}s
 * that determine when results from the {@link Window} are emitted. Each emitted result contains one or more
 * messages in the window and is called a {@link WindowPane}.
 *
 * <p> A window can have early triggers that allow emitting {@link WindowPane}s speculatively before all data for the window
 * has arrived or late triggers that allow handling of late data arrivals.
 *
 *                                     window wk1
 *                                      +--------------------------------+
 *                                      ------------+--------+-----------+
 *                                      |           |        |           |
 *                                      | pane 1    |pane2   |   pane3   |
 *                                      +-----------+--------+-----------+
 *
 -----------------------------------
 *incoming message stream ------+
 -----------------------------------
 *                                      window wk2
 *                                      +---------------------+---------+
 *                                      |   pane 1|   pane 2  |  pane 3 |
 *                                      |         |           |         |
 *                                      +---------+-----------+---------+
 *
 *                                      window wk3
 *                                      +----------+-----------+---------+
 *                                      |          |           |         |
 *                                      | pane 1   |  pane 2   |   pane 3|
 *                                      |          |           |         |
 *                                      +----------+-----------+---------+
 *
 *
 * <p> A {@link Window} can be one of the following types:
 * <ul>
 *   <li>
 *     Tumbling Windows: A tumbling window defines a series of non-overlapping, fixed size, contiguous intervals.
 *   <li>
 *     Session Windows: A session window groups a {@link org.apache.samza.operators.MessageStream} into sessions.
 *     A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
 *     The boundary for a session is defined by a {@code sessionGap}. All messages that that arrive within
 *     the gap are grouped into the same session.
 *   <li>
 *     Global Windows: A global window defines a single infinite window over the entire {@link org.apache.samza.operators.MessageStream}.
 *     An early trigger must be specified when defining a global window.
 * </ul>
 *
 * <p> A {@link Window} is defined as "keyed" when the incoming messages are first grouped based on their key
 * and triggers are fired and window panes are emitted per-key. It is possible to construct "keyed" variants of all the above window
 * types.
 *
 */
@InterfaceStability.Unstable
public final class Windows {

  private Windows() { }

  /**
   * Creates a {@link Window} that groups incoming messages into fixed-size, non-overlapping processing
   * time based windows based on the provided keyFn and applies the provided fold function to them.
   *
   * <p>The below example computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    Function<UserClick, String> keyFn = ...;
   *    BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *    MessageStream<WindowPane<String, Integer>> windowedStream = stream.window(
   *        Windows.keyedTumblingWindow(keyFn, Duration.ofSeconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param interval the duration in processing time
   * @param foldFn the function to aggregate messages in the {@link WindowPane}
   * @param <M> the type of the input message
   * @param <WV> the type of the {@link WindowPane} output value
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function.
   */
  public static <M, K, WV> Window<M, K, WV>
    keyedTumblingWindow(Function<M, K> keyFn, Duration interval, BiFunction<M, WV, WV> foldFn) {

    Trigger<M> defaultTrigger = new TimeTrigger<>(interval);
    return new WindowInternal<M, K, WV>(defaultTrigger, foldFn, keyFn, null);
  }


  /**
   * Creates a {@link Window} that groups incoming messages into fixed-size, non-overlapping
   * processing time based windows using the provided keyFn.
   *
   * <p>The below example groups the stream into fixed-size 10 second windows for each key.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    Function<UserClick, String> keyFn = ...;
   *    MessageStream<WindowPane<String, Collection<UserClick>>> windowedStream = stream.window(
   *        Windows.keyedTumblingWindow(keyFn, Duration.ofSeconds(10)));
   * }
   * </pre>
   *
   * @param keyFn function to extract key from the message
   * @param interval the duration in processing time
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function
   */
  public static <M, K> Window<M, K, Collection<M>> keyedTumblingWindow(Function<M, K> keyFn, Duration interval) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedTumblingWindow(keyFn, interval, aggregator);
  }

  /**
   * Creates a {@link Window} that windows values into fixed-size processing time based windows and aggregates
   * them applying the provided function.
   *
   * <p>The below example computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   *    MessageStream<String> stream = ...;
   *    BiFunction<String, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *    MessageStream<WindowPane<Void, Integer>> windowedStream = stream.window(
   *        Windows.tumblingWindow(Duration.ofSeconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param duration the duration in processing time
   * @param foldFn to aggregate messages in the {@link WindowPane}
   * @param <M> the type of the input message
   * @param <WV> the type of the {@link WindowPane} output value
   * @return the created {@link Window} function
   */
  public static <M, WV> Window<M, Void, WV>
    tumblingWindow(Duration duration, BiFunction<M, WV, WV> foldFn) {
    Trigger<M> defaultTrigger = Triggers.repeat(new TimeTrigger<>(duration));
    return new WindowInternal<>(defaultTrigger, foldFn, null, null);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into fixed-size, non-overlapping
   * processing time based windows.
   *
   * <p>The below example groups the stream into fixed-size 10 second windows and computes a windowed-percentile.
   *
   * <pre> {@code
   *    MessageStream<Long> stream = ...;
   *    Function<Collection<Long, Long>> percentile99 = ..
   *
   *    MessageStream<WindowPane<Void, Collection<Long>>> windowedStream = integerStream.window(Windows.tumblingWindow(Duration.ofSeconds(10)));
   *    MessageStream<Long> windowedPercentiles = windowed.map(windowedOutput -> percentile99(windowedOutput.getMessage());
   * }
   * </pre>
   *
   * @param duration the duration in processing time
   * @param <M> the type of the input message
   * @return the created {@link Window} function
   */
  public static <M> Window<M, Void, Collection<M>> tumblingWindow(Duration duration) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return tumblingWindow(duration, aggregator);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into sessions per-key based on the provided {@code sessionGap}
   * and applies the provided fold function to them.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
   * A session is considered complete when no new messages arrive within the {@code sessionGap}. All messages that arrive within
   * the gap are grouped into the same session.
   *
   * <p>The below example computes the maximum value per-key over a session window of gap 10 seconds.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseInt(m), c);
   *    Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *    MessageStream<WindowPane<String, Integer>> windowedStream = stream.window(
   *        Windows.keyedSessionWindow(userIdExtractor, Duration.minute(1), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param sessionGap the timeout gap for defining the session
   * @param foldFn the function to aggregate messages in the {@link WindowPane}
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @param <WV> the type of the output value in the {@link WindowPane}
   * @return the created {@link Window} function
   */
  public static <M, K, WV> Window<M, K, WV> keyedSessionWindow(Function<M, K> keyFn, Duration sessionGap, BiFunction<M, WV, WV> foldFn) {
    Trigger<M> defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);
    return new WindowInternal<>(defaultTrigger, foldFn, keyFn, null);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into sessions per-key based on the provided {@code sessionGap}.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}. The
   * boundary for the session is defined by a {@code sessionGap}. All messages that that arrive within
   * the gap are grouped into the same session.
   *
   * <p>The below example groups the stream into per-key session windows of gap 10 seconds.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    BiFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseIntField(m), c);
   *    Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *    MessageStream<WindowPane<String>, Collection<M>> windowedStream = stream.window(
   *        Windows.keyedSessionWindow(userIdExtractor, Duration.ofSeconds(10)));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message}
   * @param sessionGap the timeout gap for defining the session
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function
   */
  public static <M, K> Window<M, K, Collection<M>> keyedSessionWindow(Function<M, K> keyFn, Duration sessionGap) {

    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedSessionWindow(keyFn, sessionGap, aggregator);
  }


  /**
   * Creates a {@link Window} that groups incoming messages into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting an early trigger.
   *
   * <p>The below example computes the maximum value over a count based window. The window emits {@link WindowPane}s when
   * there are either 50 messages in the window pane or when 10 seconds have passed since the first message in the pane.
   *
   * <pre> {@code
   *    MessageStream<Long> stream = ...;
   *    BiFunction<Long, Long, Long> maxAggregator = (m, c)-> Math.max(m, c);
   *    MessageStream<WindowPane<Void, Long>> windowedStream = stream.window(Windows.globalWindow(maxAggregator)
   *      .setEarlyTriggers(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.ofSeconds(10))))))
   * }
   * </pre>
   *
   * @param foldFn the function to aggregate messages in the {@link WindowPane}
   * @param <M> the type of message
   * @param <WV> type of the output value in the {@link WindowPane}
   * @return the created {@link Window} function.
   */
  public static <M, WV> Window<M, Void, WV> globalWindow(BiFunction<M, WV, WV> foldFn) {
    return new WindowInternal<>(null, foldFn, null, null);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into a single global window. This window does not have a
   * default trigger. The triggering behavior must be specified by setting an early trigger.
   *
   * The below example groups the stream into count based windows that trigger every 50 messages or every 10 minutes.
   * <pre> {@code
   *    MessageStream<Long> stream = ...;
   *    MessageStream<WindowPane<Void, Collection<Long>> windowedStream = stream.window(Windows.globalWindow()
   *      .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.ofSeconds(10))))))
   * }
   * </pre>
   *
   * @param <M> the type of message
   * @return the created {@link Window} function.
   */
  public static <M> Window<M, Void, Collection<M>> globalWindow() {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return globalWindow(aggregator);
  }

  /**
   * Returns a global {@link Window} that groups incoming messages using the provided keyFn.
   * The window does not have a default trigger. The triggering behavior must be specified by setting an early
   * trigger.
   *
   * <p> The below example groups the stream into count based windows. The window triggers every 50 messages or every
   * 10 minutes.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    BiFunction<UserClick, Long, Long> maxAggregator = (m, c)-> Math.max(parseLongField(m), c);
   *    Function<UserClick, String> keyFn = ...;
   *    MessageStream<WindowPane<String, Long>> windowedStream = stream.window(Windows.keyedGlobalWindow(keyFn, maxAggregator)
   *      .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param foldFn the function to aggregate messages in the {@link WindowPane}
   * @param <M> the type of message
   * @param <K> type of the key in the {@link Window}
   * @param <WV> the type of the output value in the {@link WindowPane}
   * @return the created {@link Window} function
   */
  public static <M, K, WV> Window<M, K, WV> keyedGlobalWindow(Function<M, K> keyFn, BiFunction<M, WV, WV> foldFn) {
    return new WindowInternal<M, K, WV>(null, foldFn, keyFn, null);
  }

  /**
   * Returns a global {@link Window} that groups incoming messages using the provided keyFn.
   * The window does not have a default trigger. The triggering behavior must be specified by setting an early trigger.
   *
   * <p> The below example groups the stream per-key into count based windows. The window triggers every 50 messages or
   * every 10 minutes.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    Function<UserClick, String> keyFn = ...;
   *    MessageStream<WindowPane<String, Collection<UserClick>> windowedStream = stream.window(Windows.keyedGlobalWindow(keyFn)
   *      .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.minutes(10))))))
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param <M> the type of message
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function
   */
  public static <M, K> Window<M, K, Collection<M>> keyedGlobalWindow(Function<M, K> keyFn) {
    BiFunction<M, Collection<M>, Collection<M>> aggregator = (m, c) -> {
      c.add(m);
      return c;
    };
    return keyedGlobalWindow(keyFn, aggregator);
  }
}
