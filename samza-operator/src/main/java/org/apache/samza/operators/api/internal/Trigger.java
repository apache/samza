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
package org.apache.samza.operators.api.internal;

import org.apache.samza.operators.api.WindowState;
import org.apache.samza.operators.api.data.Message;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Defines the trigger functions for {@link Operators.WindowOperator}. This class is immutable.
 *
 * @param <M>  the type of message from the input stream
 * @param <S>  the type of state variable in the window's state store
 */
public class Trigger<M extends Message, S extends WindowState> {

  /**
   * System timer based trigger condition. This is the only guarantee that the {@link Operators.WindowOperator} will proceed forward
   */
  private final Function<S, Boolean> timerTrigger;

  /**
   * early trigger condition that determines when to send the first output from the {@link Operators.WindowOperator}
   */
  private final BiFunction<M, S, Boolean> earlyTrigger;

  /**
   * late trigger condition that determines when to send the updated output after the first one from a {@link Operators.WindowOperator}
   */
  private final BiFunction<M, S, Boolean> lateTrigger;

  /**
   * the function to updated the window state when the first output is triggered
   */
  private final Function<S, S> earlyTriggerUpdater;

  /**
   * the function to updated the window state when the late output is triggered
   */
  private final Function<S, S> lateTriggerUpdater;

  /**
   * Private constructor to prevent instantiation
   *
   * @param timerTrigger  system timer trigger condition
   * @param earlyTrigger  early trigger condition
   * @param lateTrigger   late trigger condition
   * @param earlyTriggerUpdater  early trigger state updater
   * @param lateTriggerUpdater   late trigger state updater
   */
  private Trigger(Function<S, Boolean> timerTrigger, BiFunction<M, S, Boolean> earlyTrigger, BiFunction<M, S, Boolean> lateTrigger,
      Function<S, S> earlyTriggerUpdater, Function<S, S> lateTriggerUpdater) {
    this.timerTrigger = timerTrigger;
    this.earlyTrigger = earlyTrigger;
    this.lateTrigger = lateTrigger;
    this.earlyTriggerUpdater = earlyTriggerUpdater;
    this.lateTriggerUpdater = lateTriggerUpdater;
  }

  /**
   * Static method to create a {@link Trigger} object
   *
   * @param timerTrigger  system timer trigger condition
   * @param earlyTrigger  early trigger condition
   * @param lateTrigger  late trigger condition
   * @param earlyTriggerUpdater  early trigger state updater
   * @param lateTriggerUpdater  late trigger state updater
   * @param <M>  the type of input {@link Message}
   * @param <S>  the type of window state extends {@link WindowState}
   * @return  the {@link Trigger} function
   */
  public static <M extends Message, S extends WindowState> Trigger<M, S> createTrigger(Function<S, Boolean> timerTrigger,
      BiFunction<M, S, Boolean> earlyTrigger, BiFunction<M, S, Boolean> lateTrigger, Function<S, S> earlyTriggerUpdater,
      Function<S, S> lateTriggerUpdater) {
    return new Trigger(timerTrigger, earlyTrigger, lateTrigger, earlyTriggerUpdater, lateTriggerUpdater);
  }
}
