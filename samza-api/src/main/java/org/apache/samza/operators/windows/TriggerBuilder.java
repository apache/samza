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
import org.apache.samza.operators.data.Message;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * This class defines a builder of {@link Trigger} object for a {@link Window}. The triggers are categorized into
 * three types:
 *
 * <p>
 *   early trigger: defines the condition when the first output from the window function is sent.
 *   late trigger: defines the condition when the updated output after the first output is sent.
 *   timer trigger: defines a system timeout condition to trigger output if no more inputs are received to enable early/late triggers
 * </p>
 *
 * If multiple conditions are defined for a specific type of trigger, the aggregated trigger is the disjunction
 * of each individual trigger (i.e. OR).
 *
 * @param <M>  the type of input {@link Message} to the {@link Window}
 * @param <V>  the type of output value from the {@link Window}
 */
@InterfaceStability.Unstable
public final class TriggerBuilder<M extends Message, V> {

  /**
   * Predicate helper to OR multiple trigger conditions
   */
  static class PredicateHelper {
    static <M, S> BiFunction<M, S, Boolean> or(BiFunction<M, S, Boolean> lhs, BiFunction<M, S, Boolean> rhs) {
      return (m, s) -> lhs.apply(m, s) || rhs.apply(m, s);
    }

    static <S> Function<S, Boolean> or(Function<S, Boolean> lhs, Function<S, Boolean> rhs) {
      return s -> lhs.apply(s) || rhs.apply(s);
    }
  }

  /**
   * The early trigger condition that determines the first output from the {@link Window}
   */
  private BiFunction<M, WindowState<V>, Boolean> earlyTrigger = null;

  /**
   * The late trigger condition that determines the late output(s) from the {@link Window}
   */
  private BiFunction<M, WindowState<V>, Boolean> lateTrigger = null;

  /**
   * The system timer based trigger conditions that guarantees the {@link Window} proceeds forward
   */
  private Function<WindowState<V>, Boolean> timerTrigger = null;

  /**
   * The state updater function to be applied after the first output is triggered
   */
  private Function<WindowState<V>, WindowState<V>> earlyTriggerUpdater = Function.identity();

  /**
   * The state updater function to be applied after the late output is triggered
   */
  private Function<WindowState<V>, WindowState<V>> lateTriggerUpdater = Function.identity();

  /**
   * Helper method to add a trigger condition
   *
   * @param currentTrigger  current trigger condition
   * @param newTrigger  new trigger condition
   * @return  combined trigger condition that is {@code currentTrigger} OR {@code newTrigger}
   */
  private BiFunction<M, WindowState<V>, Boolean> addTrigger(BiFunction<M, WindowState<V>, Boolean> currentTrigger,
      BiFunction<M, WindowState<V>, Boolean> newTrigger) {
    if (currentTrigger == null) {
      return newTrigger;
    }

    return PredicateHelper.or(currentTrigger, newTrigger);
  }

  /**
   * Helper method to add a system timer trigger
   *
   * @param currentTimer  current timer condition
   * @param newTimer  new timer condition
   * @return  combined timer condition that is {@code currentTimer} OR {@code newTimer}
   */
  private Function<WindowState<V>, Boolean> addTimerTrigger(Function<WindowState<V>, Boolean> currentTimer,
      Function<WindowState<V>, Boolean> newTimer) {
    if (currentTimer == null) {
      return newTimer;
    }

    return PredicateHelper.or(currentTimer, newTimer);
  }

  /**
   * default constructor to prevent instantiation
   */
  private TriggerBuilder() {}

  /**
   * Constructor that set the size limit as the early trigger for a window
   *
   * @param sizeLimit  the number of messages in a window that would trigger the first output
   */
  private TriggerBuilder(long sizeLimit) {
    this.earlyTrigger = (m, s) -> s.getNumberMessages() > sizeLimit;
  }

  /**
   * Constructor that set the event time length as the early trigger
   *
   * @param eventTimeFunction  the function that calculate the event time in nano-second from the input {@link Message}
   * @param wndLenMs  the window length in event time in milli-second
   */
  private TriggerBuilder(Function<M, Long> eventTimeFunction, long wndLenMs) {
    this.earlyTrigger = (m, s) ->
        TimeUnit.NANOSECONDS.toMillis(Math.max(s.getLatestEventTimeNs() - s.getEarliestEventTimeNs(),
            eventTimeFunction.apply(m) - s.getEarliestEventTimeNs())) > wndLenMs;
  }

  /**
   * Constructor that set the special token message as the early trigger
   *
   * @param tokenFunc  the function that checks whether an input {@link Message} is a token message that triggers window output
   */
  private TriggerBuilder(Function<M, Boolean> tokenFunc) {
    this.earlyTrigger = (m, s) -> tokenFunc.apply(m);
  }

  /**
   * Build method that creates an {@link Trigger} object based on the trigger conditions set in {@link TriggerBuilder}
   * This is kept package private and only used by {@link Windows} to convert the mutable {@link TriggerBuilder} object to an immutable {@link Trigger} object
   *
   * @return  the final {@link Trigger} object
   */
  Trigger<M, WindowState<V>> build() {
    return Trigger.createTrigger(this.timerTrigger, this.earlyTrigger, this.lateTrigger, this.earlyTriggerUpdater, this.lateTriggerUpdater);
  }

  /**
   * Public API methods start here
   */


  /**
   * API method to allow users to set an update method to update the output value after the first window output is triggered
   * by the early trigger condition
   *
   * @param onTriggerFunc  the method to update the output value after the early trigger
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> onEarlyTrigger(Function<V, V> onTriggerFunc) {
    this.earlyTriggerUpdater = s -> {
      s.setOutputValue(onTriggerFunc.apply(s.getOutputValue()));
      return s;
    };
    return this;
  }

  /**
   * API method to allow users to set an update method to update the output value after a late window output is triggered
   * by the late trigger condition
   *
   * @param onTriggerFunc  the method to update the output value after the late trigger
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> onLateTrigger(Function<V, V> onTriggerFunc) {
    this.lateTriggerUpdater = s -> {
      s.setOutputValue(onTriggerFunc.apply(s.getOutputValue()));
      return s;
    };
    return this;
  }

  /**
   * API method to allow users to add a system timer trigger based on timeout after the last message received in the window
   *
   * @param timeoutMs  the timeout in ms after the last message received in the window
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> addTimeoutSinceLastMessage(long timeoutMs) {
    this.timerTrigger = this.addTimerTrigger(this.timerTrigger,
        s -> TimeUnit.NANOSECONDS.toMillis(s.getLastMessageTimeNs()) + timeoutMs < System.currentTimeMillis());
    return this;
  }

  /**
   * API method to allow users to add a system timer trigger based on the timeout after the first message received in the window
   *
   * @param timeoutMs  the timeout in ms after the first message received in the window
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> addTimeoutSinceFirstMessage(long timeoutMs) {
    this.timerTrigger = this.addTimerTrigger(this.timerTrigger, s ->
        TimeUnit.NANOSECONDS.toMillis(s.getFirstMessageTimeNs()) + timeoutMs < System.currentTimeMillis());
    return this;
  }

  /**
   * API method allow users to add a late trigger based on the window size limit
   *
   * @param sizeLimit  limit on the number of messages in window
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> addLateTriggerOnSizeLimit(long sizeLimit) {
    this.lateTrigger = this.addTrigger(this.lateTrigger, (m, s) -> s.getNumberMessages() > sizeLimit);
    return this;
  }

  /**
   * API method to allow users to define a customized late trigger function based on input message and the window state
   *
   * @param lateTrigger  the late trigger condition based on input {@link Message} and the current {@link WindowState}
   * @return  the {@link TriggerBuilder} object
   */
  public TriggerBuilder<M, V> addLateTrigger(BiFunction<M, WindowState<V>, Boolean> lateTrigger) {
    this.lateTrigger = this.addTrigger(this.lateTrigger, lateTrigger);
    return this;
  }

  /**
   * Static API method to create a {@link TriggerBuilder} w/ early trigger condition based on window size limit
   *
   * @param sizeLimit  window size limit
   * @param <M>  the type of input {@link Message}
   * @param <V>  the type of {@link Window} output value
   * @return  the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> earlyTriggerWhenExceedWndLen(long sizeLimit) {
    return new TriggerBuilder<M, V>(sizeLimit);
  }

  /**
   * Static API method to create a {@link TriggerBuilder} w/ early trigger condition based on event time window
   *
   *
   * @param eventTimeFunc  the function to get the event time from the input message
   * @param eventTimeWndSizeMs  the event time window size in Ms
   * @param <M>  the type of input {@link Message}
   * @param <V>  the type of {@link Window} output value
   * @return  the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> earlyTriggerOnEventTime(Function<M, Long> eventTimeFunc, long eventTimeWndSizeMs) {
    return new TriggerBuilder<M, V>(eventTimeFunc, eventTimeWndSizeMs);
  }

  /**
   * Static API method to create a {@link TriggerBuilder} w/ early trigger condition based on token messages
   *
   * @param tokenFunc  the function to determine whether an input message is a window token or not
   * @param <M>  the type of input {@link Message}
   * @param <V>  the type of {@link Window} output value
   * @return  the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> earlyTriggerOnTokenMsg(Function<M, Boolean> tokenFunc) {
    return new TriggerBuilder<M, V>(tokenFunc);
  }

  /**
   * Static API method to allow customized early trigger condition based on input {@link Message} and the corresponding {@link WindowState}
   *
   * @param earlyTrigger  the user defined early trigger condition
   * @param <M>   the input message type
   * @param <V>   the output value from the window
   * @return   the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> earlyTrigger(BiFunction<M, WindowState<V>, Boolean> earlyTrigger) {
    TriggerBuilder<M, V> newTriggers =  new TriggerBuilder<M, V>();
    newTriggers.earlyTrigger = newTriggers.addTrigger(newTriggers.earlyTrigger, earlyTrigger);
    return newTriggers;
  }

  /**
   * Static API method to create a {@link TriggerBuilder} w/ system timeout after the last message received in the window
   *
   * @param timeoutMs  timeout in ms after the last message received
   * @param <M>  the type of input {@link Message}
   * @param <V>  the type of {@link Window} output value
   * @return  the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> timeoutSinceLastMessage(long timeoutMs) {
    return new TriggerBuilder<M, V>().addTimeoutSinceLastMessage(timeoutMs);
  }

  /**
   * Static API method to create a {@link TriggerBuilder} w/ system timeout after the first message received in the window
   *
   * @param timeoutMs  timeout in ms after the first message received
   * @param <M>  the type of input {@link Message}
   * @param <V>  the type of {@link Window} output value
   * @return  the {@link TriggerBuilder} object
   */
  public static <M extends Message, V> TriggerBuilder<M, V> timeoutSinceFirstMessage(long timeoutMs) {
    return new TriggerBuilder<M, V>().addTimeoutSinceFirstMessage(timeoutMs);
  }
}
