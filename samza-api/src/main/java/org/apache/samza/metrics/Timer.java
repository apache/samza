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

package org.apache.samza.metrics;

import org.apache.samza.util.Clock;

/**
 * A timer metric that stores time duration and provides {@link Snapshot} of the
 * durations.
 */
public class Timer implements Metric {

  private final String name;
  private final Reservoir reservoir;

  /**
   * Default constructor. It uses {@link SlidingTimeWindowReservoir} as the
   * default reservoir.
   *
   * @param name name of this timer
   */
  public Timer(String name) {
    this(name, new SlidingTimeWindowReservoir());
  }

  /**
   * Construct a {@link Timer} with given window size
   *
   * @param name name of this timer
   * @param windowMs the window size. unit is millisecond
   * @param clock the clock for the reservoir
   */
  public Timer(String name, long windowMs, Clock clock) {
    this(name, new SlidingTimeWindowReservoir(windowMs, clock));
  }

  /**
   * Construct a {@link Timer} with given window size and collision buffer
   *
   * @param name name of this timer
   * @param windowMs the window size. unit is millisecond
   * @param collisionBuffer amount of collisions allowed in one millisecond.
   * @param clock the clock for the reservoir
   */
  public Timer(String name, long windowMs, int collisionBuffer, Clock clock) {
    this(name, new SlidingTimeWindowReservoir(windowMs, collisionBuffer, clock));
  }

  /**
   * Construct a {@link Timer} with given {@link Reservoir}
   *
   * @param name name of this timer
   * @param reservoir the given reservoir
   */
  public Timer(String name, Reservoir reservoir) {
    this.name = name;
    this.reservoir = reservoir;
  }

  /**
   * Add the time duration
   *
   * @param duration time duration
   */
  public void update(long duration) {
    if (duration > 0) {
      reservoir.update(duration);
    }
  }

  /**
   * Get the {@link Snapshot}
   *
   * @return a statistical snapshot
   */
  public Snapshot getSnapshot() {
    return reservoir.getSnapshot();
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.timer(this);
  }

  /**
   * Get the name of the timer
   *
   * @return name of the timer
   */
  public String getName() {
    return name;
  }
}
