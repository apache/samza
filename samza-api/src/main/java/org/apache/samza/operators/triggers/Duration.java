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

package org.apache.samza.operators.triggers;

import java.util.concurrent.TimeUnit;

/**
 * Specifies a time interval for applying a window transform over a {@link org.apache.samza.operators.MessageStream}.
 * Users should rely on the factory methods to create instances of this class.
 *
 * <pre> {@code
 *  Duration threeDays = Duration.of(3, TimeUnit.DAYS);
 *  Duration threeMins = Duration.minutes(3);
 *  Duration threeSeconds = Duration.of(3, TimeUnit.SECONDS);
 *  MessageStream<WindowOutput<>> windowed = integerStream.window(Windows.tumblingWindow(threeMins));
 * }
 * </pre>
 */
public final class Duration {

  private final long value;

  private final TimeUnit timeUnit;

  private final Duration.TimeCharacteristic characteristic = Duration.TimeCharacteristic.PROCESSING_TIME;

  /*
   * Should this {@link Time} be specified in terms of event time or processing time.
   */
  enum TimeCharacteristic {
    EVENT_TIME, PROCESSING_TIME
  }

  /**
   * Private constructor to allow instantiation only via factory methods
   */
  private Duration(long value, TimeUnit timeunit) {
    this.timeUnit = timeunit;
    this.value = value;
  }

  public TimeCharacteristic getTimeCharacteristic() {
    return characteristic;
  }

  public long toMilliseconds() {
    return timeUnit.toMillis(value);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided time unit.
   * @param value the value for the time unit.
   * @param unit the duration granularity.
   * @return the created instance.
   */

  public static Duration of(long value, TimeUnit unit) {
    return new Duration(value, unit);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided days.
   * @param days the provided number of days.
   * @return the created instance.
   */
  public static Duration days(long days) {
    return of(days, TimeUnit.DAYS);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided hours.
   * @param hours the provided number of hours.
   * @return the created instance.
   */
  public static Duration hours(long hours) {
    return of(hours, TimeUnit.HOURS);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided minutes.
   * @param min the provided number of minutes.
   * @return the created instance.
   */
  public static Duration minutes(long min) {
    return of(min, TimeUnit.MINUTES);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided seconds.
   * @param sec the provided number of minutes.
   * @return the created instance.
   */
  public static Duration seconds(long sec) {
    return of(sec, TimeUnit.SECONDS);
  }

  /**
   * Creates a new {@link Duration} that specifies the provided milliseconds.
   * @param millis the provided number of minutes.
   * @return the created instance.
   */
  public static Duration milliseconds(long millis) {
    return of(millis, TimeUnit.MILLISECONDS);
  }

}
