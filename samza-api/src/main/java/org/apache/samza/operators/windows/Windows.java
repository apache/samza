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
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.triggers.TimeTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.apache.samza.serializers.Serde;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * APIs for creating different types of {@link Window}s.
 *
 * Groups incoming messages in a {@link org.apache.samza.operators.MessageStream} into finite windows for processing.
 *
 * <p> Each window is uniquely identified by its {@link WindowKey}. A window can have one or more associated
 * {@link Trigger}s that determine when results from the {@link Window} are emitted. Each emitted result contains one
 * or more messages in the window and is called a {@link WindowPane}.
 *
 * <p> A window can have early triggers that allow emitting {@link WindowPane}s speculatively before all data
 * for the window has arrived, or late triggers that allow handling late arrivals of data.
 * <pre>
 *                                     window wk1
 *                                      +--------------------------------+
 *                                      ------------+--------+-----------+
 *                                      |           |        |           |
 *                                      | pane 1    |pane2   |   pane3   |
 *                                      +-----------+--------+-----------+
 *
 * -----------------------------------
 *     incoming message stream ------+
 * -----------------------------------
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
 * </pre>
 * <p> A {@link Window} can be one of the following types:
 * <ul>
 *   <li>
 *     Tumbling Window: A tumbling window defines a series of non-overlapping, fixed size, contiguous intervals.
 *   <li>
 *     Session Window: A session window groups a {@link org.apache.samza.operators.MessageStream} into sessions.
 *     A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
 *     The boundary for a session is defined by a {@code sessionGap}. All messages that that arrive within
 *     the gap are grouped into the same session.
 * </ul>
 *
 * <p> A {@link Window} is said to be "keyed" when the incoming messages are first grouped based on their key
 * and triggers are fired and window panes are emitted per-key. It is possible to construct "keyed" variants
 * of the window types above.
 *
 * <p> The value for the window can be updated incrementally by providing an {@code initialValue} {@link Supplier}
 * and an aggregating {@link FoldLeftFunction}. The initial value supplier is invoked every time a new window is
 * created. The aggregating function is invoked for each incoming message for the window. If these are not provided,
 * the emitted {@link WindowPane} will contain a collection of messages in the window.
 *
 * <p> Time granularity for windows: Currently, time durations are always measured in milliseconds. Time units of
 * finer granularity are not supported.
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
   *    Supplier<Integer> initialValue = () -> 0;
   *    FoldLeftFunction<UserClick, Integer, Integer> maxAggregator = (m, c) -> Math.max(parseInt(m), c);
   *    MessageStream<WindowPane<String, Integer>> windowedStream = stream.window(
   *        Windows.keyedTumblingWindow(keyFn, Duration.ofSeconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param interval the duration in processing time
   * @param initialValue the initial value supplier for the aggregator. Invoked when a new window is created.
   * @param aggregator the function to incrementally update the window value. Invoked when a new message
   *                   arrives for the window.
   * @param <M> the type of the input message
   * @param <WV> the type of the {@link WindowPane} output value
   * @param <K> the type of the key in the {@link Window}
   * @param keySerde the serde for the window key
   * @param wvSerde the serde for the window value
   * @return the created {@link Window} function.
   */
  public static <M, K, WV> Window<M, K, WV> keyedTumblingWindow(
      Function<? super M, ? extends K> keyFn, Duration interval,
      Supplier<? extends WV> initialValue, FoldLeftFunction<? super M, WV> aggregator, Serde<K> keySerde, Serde<WV> wvSerde) {

    Trigger<M> defaultTrigger = new TimeTrigger<>(interval);
    return new WindowInternal<>(defaultTrigger, (Supplier<WV>) initialValue, (FoldLeftFunction<M, WV>) aggregator,
        (Function<M, K>) keyFn, null, WindowType.TUMBLING, keySerde, wvSerde, null);
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
   * @param keySerde the serde for the window key
   * @param msgSerde the serde for the input message
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function
   */
  public static <M, K> Window<M, K, Collection<M>> keyedTumblingWindow(
      Function<M, K> keyFn, Duration interval, Serde<K> keySerde, Serde<M> msgSerde) {

    Trigger<M> defaultTrigger = new TimeTrigger<>(interval);
    return new WindowInternal<>(defaultTrigger, null, null, keyFn, null,
        WindowType.TUMBLING, keySerde, null, msgSerde);
  }

  /**
   * Creates a {@link Window} that windows values into fixed-size processing time based windows and aggregates
   * them applying the provided function.
   *
   * <p>The below example computes the maximum value per-key over fixed size 10 second windows.
   *
   * <pre> {@code
   *    MessageStream<String> stream = ...;
   *    Supplier<Integer> initialValue = () -> 0;
   *    FoldLeftFunction<String, Integer, Integer> maxAggregator = (m, c) -> Math.max(parseInt(m), c);
   *    MessageStream<WindowPane<Void, Integer>> windowedStream = stream.window(
   *        Windows.tumblingWindow(Duration.ofSeconds(10), maxAggregator));
   * }
   * </pre>
   *
   * @param interval the duration in processing time
   * @param initialValue the initial value supplier for the aggregator. Invoked when a new window is created.
   * @param aggregator the function to incrementally update the window value. Invoked when a new message
   *                   arrives for the window.
   * @param wvSerde the serde for the window value
   * @param <M> the type of the input message
   * @param <WV> the type of the {@link WindowPane} output value
   * @return the created {@link Window} function
   */
  public static <M, WV> Window<M, Void, WV> tumblingWindow(Duration interval, Supplier<? extends WV> initialValue,
      FoldLeftFunction<? super M, WV> aggregator, Serde<WV> wvSerde) {
    Trigger<M> defaultTrigger = new TimeTrigger<>(interval);
    return new WindowInternal<>(defaultTrigger, (Supplier<WV>) initialValue, (FoldLeftFunction<M, WV>) aggregator,
        null, null, WindowType.TUMBLING, null, wvSerde, null);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into fixed-size, non-overlapping
   * processing time based windows.
   *
   * <p>The below example groups the stream into fixed-size 10 second windows and computes a windowed-percentile.
   *
   * <pre> {@code
   *    MessageStream<Long> stream = ...;
   *    Function<Collection<Long>, Long> percentile99 = ..
   *
   *    MessageStream<WindowPane<Void, Collection<Long>>> windowedStream =
   *        integerStream.window(Windows.tumblingWindow(Duration.ofSeconds(10)));
   *    MessageStream<Long> windowedPercentiles =
   *        windowedStream.map(windowPane -> percentile99(windowPane.getMessage());
   * }
   * </pre>
   *
   * @param duration the duration in processing time
   * @param msgSerde the serde for the input message
   * @param <M> the type of the input message
   *
   * @return the created {@link Window} function
   */
  public static <M> Window<M, Void, Collection<M>> tumblingWindow(Duration duration, Serde<M> msgSerde) {
    Trigger<M> defaultTrigger = new TimeTrigger<>(duration);

    return new WindowInternal<>(defaultTrigger, null, null, null,
       null, WindowType.TUMBLING, null, null, msgSerde);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into sessions per-key based on the provided
   * {@code sessionGap} and applies the provided fold function to them.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}.
   * A session is considered complete when no new messages arrive within the {@code sessionGap}. All messages
   * that arrive within the gap are grouped into the same session.
   *
   * <p>The below example computes the maximum value per-key over a session window of gap 10 seconds.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    Supplier<Integer> initialValue = () -> 0;
   *    FoldLeftFunction<UserClick, Integer, Integer> maxAggregator = (m, c) -> Math.max(parseInt(m), c);
   *    Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *    MessageStream<WindowPane<String, Integer>> windowedStream = stream.window(
   *        Windows.keyedSessionWindow(userIdExtractor, Duration.minute(1), maxAggregator));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message
   * @param sessionGap the timeout gap for defining the session
   * @param initialValue the initial value supplier for the aggregator. Invoked when a new window is created.
   * @param aggregator the function to incrementally update the window value. Invoked when a new message
   *                   arrives for the window.
   * @param keySerde the serde for the window key
   * @param wvSerde the serde for the window value
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @param <WV> the type of the output value in the {@link WindowPane}
   * @return the created {@link Window} function
   */
  public static <M, K, WV> Window<M, K, WV> keyedSessionWindow(
      Function<? super M, ? extends K> keyFn, Duration sessionGap,
      Supplier<? extends WV> initialValue, FoldLeftFunction<? super M, WV> aggregator, Serde<K> keySerde, Serde<WV> wvSerde) {
    Trigger<M> defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);
    return new WindowInternal<>(defaultTrigger, (Supplier<WV>) initialValue, (FoldLeftFunction<M, WV>) aggregator,
        (Function<M, K>) keyFn, null, WindowType.SESSION, keySerde, wvSerde, null);
  }

  /**
   * Creates a {@link Window} that groups incoming messages into sessions per-key based on the provided
   * {@code sessionGap}.
   *
   * <p>A <i>session</i> captures some period of activity over a {@link org.apache.samza.operators.MessageStream}. The
   * boundary for the session is defined by a {@code sessionGap}. All messages that that arrive within
   * the gap are grouped into the same session.
   *
   * <p>The below example groups the stream into per-key session windows of gap 10 seconds.
   *
   * <pre> {@code
   *    MessageStream<UserClick> stream = ...;
   *    Supplier<Integer> initialValue = () -> 0;
   *    FoldLeftFunction<UserClick, Integer, Integer> maxAggregator = (m, c)-> Math.max(parseIntField(m), c);
   *    Function<UserClick, String> userIdExtractor = m -> m.getUserId()..;
   *    MessageStream<WindowPane<String>, Collection<M>> windowedStream = stream.window(
   *        Windows.keyedSessionWindow(userIdExtractor, Duration.ofSeconds(10)));
   * }
   * </pre>
   *
   * @param keyFn the function to extract the window key from a message}
   * @param sessionGap the timeout gap for defining the session
   * @param msgSerde the serde for the input message
   * @param keySerde the serde for the window key
   * @param <M> the type of the input message
   * @param <K> the type of the key in the {@link Window}
   * @return the created {@link Window} function
   */
  public static <M, K> Window<M, K, Collection<M>> keyedSessionWindow(
          Function<? super M, ? extends K> keyFn, Duration sessionGap, Serde<M> msgSerde, Serde<K> keySerde) {

    Trigger<M> defaultTrigger = Triggers.timeSinceLastMessage(sessionGap);
    return new WindowInternal<>(defaultTrigger, null, null, (Function<M, K>) keyFn, null, WindowType.SESSION, keySerde, null, msgSerde);
  }
}
