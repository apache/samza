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

package org.apache.samza.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An object that schedules work to be performed and optionally slows the rate of execution.
 * By default work submitted with {@link #schedule(Runnable, long)}} will not be throttled. Work can
 * be throttled by invoking {@link #setWorkFactor(double)}.
 * <p>
 * This class is thread-safe. It must be {@link #shutdown} after use.
 */
public class ThrottlingScheduler implements Throttleable {
  private final long maxDelayNanos;
  private final ScheduledExecutorService scheduledExecutorService;
  private final HighResolutionClock clock;

  private final AtomicLong pendingNanos = new AtomicLong();
  private volatile double workToIdleFactor;

  public ThrottlingScheduler(long maxDelayMillis) {
    this.maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(maxDelayMillis);
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.clock = new SystemHighResolutionClock();
  }

  ThrottlingScheduler(long maxDelayMillis, ScheduledExecutorService scheduledExecutorService,
      HighResolutionClock clock) {
    this.maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(maxDelayMillis);
    this.scheduledExecutorService = scheduledExecutorService;
    this.clock = clock;
  }

  /**
   * This method may be used to throttle asynchronous processing by delaying the work completion callback.
   * <p>
   * Executes the given completion callback on the current thread. If throttling is enabled (the work factor
   * is less than 1.0) this method may optionally schedule the callback with a delay to satisfy the
   * requested work factor.
   *
   * @param callback the callback to complete asynchronous work
   * @param workDurationNs the duration of asynchronous work in nanoseconds
   */
  public void schedule(final Runnable callback, final long workDurationNs) {
    final double currentWorkToIdleFactor = workToIdleFactor;

    // If we're not throttling, do not get clock time, etc. This substantially reduces the overhead
    // per invocation of this feature.
    if (currentWorkToIdleFactor == 0.0) {
      callback.run();
    } else {
      final long delay = Math.min(maxDelayNanos, (long) (workDurationNs * currentWorkToIdleFactor));

      // NOTE: we accumulate pending delay nanos here, but reduce the pending delay nanos after
      // the delay operation (if applicable), so they do not continue to grow.
      addToPendingNanos(delay);

      if (pendingNanos.get() < 0) {
        callback.run();
      } else {
        final long startTimeNs = clock.nanoTime();
        scheduledExecutorService.schedule(new Runnable() {
          @Override
          public void run() {
            final long actualDelay = clock.nanoTime() - startTimeNs;
            addToPendingNanos(-actualDelay);
            callback.run();
          }
        }, delay, TimeUnit.NANOSECONDS);
      }
    }
  }

  private void addToPendingNanos(final long amount) {
    long currentValue;
    long newValue;
    do {
      currentValue = pendingNanos.get();
      newValue = MathUtil.clampAdd(currentValue, amount);
    } while (!pendingNanos.compareAndSet(currentValue, newValue));
  }

  @Override
  public void setWorkFactor(double workFactor) {
    if (workFactor < MIN_WORK_FACTOR) {
      throw new IllegalArgumentException("Work factor must be >= " + MIN_WORK_FACTOR);
    }
    if (workFactor > MAX_WORK_FACTOR) {
      throw new IllegalArgumentException("Work factor must be <= " + MAX_WORK_FACTOR);
    }

    workToIdleFactor = (1.0 - workFactor) / workFactor;
  }

  @Override
  public double getWorkFactor() {
    return 1.0 / (workToIdleFactor + 1.0);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  /**
   * Returns the total amount of delay (in nanoseconds) that needs to be applied to subsequent work.
   * Alternatively this can be thought to capture the error between expected delay and actual
   * applied delay. This accounts for variance in the precision of the delay mechanism,
   * which may vary from platform to platform.
   * <p>
   * This is required for test purposes only.
   *
   * @return the total amount of delay (in nanoseconds) that needs to be applied to subsequent work.
   */
  long getPendingNanos() {
    return pendingNanos.get();
  }

  /**
   * A convenience method for test that allows the pending delay for this executor to be set
   * explicitly.
   *
   * @param pendingNanos the pending nanos to set.
   */
  void setPendingNanos(long pendingNanos) {
    this.pendingNanos.set(pendingNanos);
  }
}
