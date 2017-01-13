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

import java.util.concurrent.TimeUnit;

/**
 * Specifies a time interval for applying a {@link Window} transform over a {@code MessageStream}. Users should
 * rely on the factory methods to create instances of this class.
 *
 * <pre> {@code
 *  Time threeDays = Time.of(3, TimeUnit.DAYS);
 *  Time threeMins = Time.minutes(3);
 *  Time threeSeconds = Time.of(3, TimeUnit.SECONDS);
 *  MessageStream<WindowOutput<WindowInfo, Integer>> windowed = integerStream.window(Windows.tumblingWindow(10000, maxAggregator);
 * }
 * </pre>
 */
public class Time {

  private final TimeUnit timeUnit;

  private final long value;

  private final Time.TimeCharacteristic characteristic = Time.TimeCharacteristic.PROCESSING_TIME;

  /*
   * Should this {@link Time} be specified in terms of event time or processing time.
   */
  enum TimeCharacteristic {
    EVENT_TIME, PROCESSING_TIME
  }

  /**
   * Private constructor to allow instantiation only via factory methods
   */
  private Time(long value, TimeUnit timeunit) {
    this.timeUnit = timeunit;
    this.value = value;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public long getValue() {
    return value;
  }

  public long toMilliseconds() {
    return timeUnit.toMillis(value);
  }


  public static Time of(long size, TimeUnit unit) {
    return new Time(size, unit);
  }

  /**
   * Creates a new {@link Time} that specifies the provided days.
   * @param days the provided number of days.
   * @return the created instance.
   */
  public static Time days(long days) {
    return of(days, TimeUnit.DAYS);
  }

  /**
   * Creates a new {@link Time} that specifies the provided hours.
   * @param hours the provided number of hours.
   * @return the created instance.
   */
  public static Time hours(long hours) {
    return of(hours, TimeUnit.HOURS);
  }

  /**
   * Creates a new {@link Time} that specifies the provided minutes.
   * @param min the provided number of minutes.
   * @return the created instance.
   */
  public static Time minutes(long min) {
    return of(min, TimeUnit.MINUTES);
  }

  /**
   * Creates a new {@link Time} that specifies the provided seconds.
   * @param sec the provided number of minutes.
   * @return the created instance.
   */
  public static Time seconds(long sec) {
    return of(sec, TimeUnit.SECONDS);
  }

  /**
   * Creates a new {@link Time} that specifies the provided milliseconds.
   * @param millis the provided number of minutes.
   * @return the created instance.
   */
  public static Time milliseconds(long millis) {
    return of(millis, TimeUnit.MILLISECONDS);
  }

}
