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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An object that performs work on the current thread and optionally slows the rate of execution.
 * By default work submitted with {@link #execute(Runnable)} will not be throttled. Work can be
 * throttled by invoking {@link #setWorkFactor(double)}.
 * <p>
 * This class is *NOT* thread-safe. It is intended to be used from a single thread. However, the
 * work factor may be set from any thread.
 */
public class ThrottlingExecutor implements Throttleable, Executor {
  private final long maxDelayNanos;
  private final HighResolutionClock clock;

  private volatile double workToIdleFactor;
  private long pendingNanos;

  public ThrottlingExecutor(long maxDelayMillis) {
    this(maxDelayMillis, new SystemHighResolutionClock());
  }

  ThrottlingExecutor(long maxDelayMillis, HighResolutionClock clock) {
    this.maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(maxDelayMillis);
    this.clock = clock;
  }

  /**
   * Executes the given command on the current thread. If throttling is enabled (the work factor
   * is less than 1.0) this command may optionally insert a delay before returning to satisfy the
   * requested work factor.
   * <p>
   * This method will not operate correctly if used by more than one thread.
   *
   * @param command the work to execute
   */
  public void execute(Runnable command) {
    final double currentWorkToIdleFactor = workToIdleFactor;

    // If we're not throttling, do not get clock time, etc. This substantially reduces the overhead
    // per invocation of this feature (by ~75%).
    if (currentWorkToIdleFactor == 0.0) {
      command.run();
    } else {
      final long startWorkNanos = clock.nanoTime();
      command.run();
      final long workNanos = clock.nanoTime() - startWorkNanos;

      // NOTE: we accumulate pending delay nanos here, but we later update the pending nanos during
      // the sleep operation (if applicable), so they do not continue to grow. We also clamp the
      // maximum sleep time to prevent excessively large sleeps between executions.
      pendingNanos = Math.min(maxDelayNanos,
          Util.clampAdd(pendingNanos, (long) (workNanos * currentWorkToIdleFactor)));
      if (pendingNanos > 0) {
        try {
          pendingNanos = sleep(pendingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
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

  /**
   * Returns the total amount of delay (in nanoseconds) that needs to be applied to subsequent work.
   * Alternatively this can be thought to capture the error between expected delay and actual
   * applied delay. This accounts for variance in the precision of the clock and the delay
   * mechanism, both of which may vary from platform to platform.
   * <p>
   * This is required for test purposes only.
   *
   * @return the total amount of delay (in nanoseconds) that needs to be applied to subsequent work.
   */
  long getPendingNanos() {
    return pendingNanos;
  }

  /**
   * A convenience method for test that allows the pending nanos for this executor to be set
   * explicitly.
   *
   * @param pendingNanos the pending nanos to set.
   */
  void setPendingNanos(long pendingNanos) {
    this.pendingNanos = pendingNanos;
  }

  /**
   * Sleeps for a period of time that approximates the requested number of nanoseconds. Actual sleep
   * time can vary significantly based on the JVM implementation and platform. This function returns
   * the measured error between expected and actual sleep time.
   *
   * @param nanos the number of nanoseconds to sleep.
   * @throws InterruptedException if the current thread is interrupted while blocked in this method.
   */
  long sleep(long nanos) throws InterruptedException {
    if (nanos <= 0) {
      return nanos;
    }

    final long start = System.nanoTime();
    TimeUnit.NANOSECONDS.sleep(nanos);

    return Util.clampAdd(nanos, -(System.nanoTime() - start));
  }
}
