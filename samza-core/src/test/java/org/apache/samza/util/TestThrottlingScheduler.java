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

import static junit.framework.Assert.*;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestThrottlingScheduler {
  private static final long MAX_NANOS = Long.MAX_VALUE;

  private static final Runnable NO_OP = new Runnable() {
    @Override
    public void run() {
      // Do nothing.
    }
  };

  private HighResolutionClock clock;
  private ScheduledExecutorService scheduledExecutorService;
  private ThrottlingScheduler throttler;

  @Before
  public void setUp() {
    clock = Mockito.mock(HighResolutionClock.class);
    scheduledExecutorService = Mockito.mock(ScheduledExecutorService.class);
    throttler = new ThrottlingScheduler(MAX_NANOS, scheduledExecutorService, clock);
  }

  @Test
  public void testInitialState() {
    ThrottlingExecutor throttler = new ThrottlingExecutor(MAX_NANOS);
    assertEquals(0, throttler.getPendingNanos());
    assertEquals(1.0, throttler.getWorkFactor());
  }

  @Test
  public void testSetWorkRate() {
    throttler.setWorkFactor(1.0);
    assertEquals(1.0, throttler.getWorkFactor());

    throttler.setWorkFactor(0.5);
    assertEquals(0.5, throttler.getWorkFactor());

    throttler.setWorkFactor(Throttleable.MIN_WORK_FACTOR);
    assertEquals(Throttleable.MIN_WORK_FACTOR, throttler.getWorkFactor());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLessThan0PercentWorkRate() {
    new ThrottlingExecutor(MAX_NANOS).setWorkFactor(-0.1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGreaterThan100PercentWorkRate() {
    new ThrottlingExecutor(MAX_NANOS).setWorkFactor(1.1);
  }

  @Test
  public void test100PercentWorkRate() throws InterruptedException {
    throttler.schedule(NO_OP, 1000);

    assertEquals(0L, throttler.getPendingNanos());

    // At 100% work rate schedule should not be called
    Mockito.verify(scheduledExecutorService, Mockito.never())
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
  }

  @Test
  public void test50PercentWorkRate() throws InterruptedException {
    throttler.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    setActualDelay(workTimeNanos);

    throttler.schedule(NO_OP, workTimeNanos);

    // Delay time is same as work time at 50% work rate
    verifyRequestedDelay(workTimeNanos);
    assertEquals(0L, throttler.getPendingNanos());
  }

  @Test
  public void testMinWorkRate() throws InterruptedException {
    final double workFactor = Throttleable.MIN_WORK_FACTOR;
    throttler.setWorkFactor(workFactor);

    // The math to work out how much to multiply work time to get expected delay time
    double workToDelayFactor = (1.0 - workFactor) / workFactor;

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long delayTimeNanos = (long) (workToDelayFactor * workTimeNanos);

    setActualDelay(delayTimeNanos);
    throttler.schedule(NO_OP, workTimeNanos);

    verifyRequestedDelay(delayTimeNanos);
    assertEquals(0, throttler.getPendingNanos());
  }

  @Test
  public void testDelayOvershoot() throws InterruptedException {
    throttler.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long expectedDelayNanos = workTimeNanos;
    final long actualDelayNanos = TimeUnit.MILLISECONDS.toNanos(6);

    setActualDelay(actualDelayNanos);

    throttler.schedule(NO_OP, workTimeNanos);

    verifyRequestedDelay(expectedDelayNanos);
    assertEquals(expectedDelayNanos - actualDelayNanos, throttler.getPendingNanos());
  }

  @Test
  public void testDelayUndershoot() throws InterruptedException {
    throttler.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long expectedDelayNanos = workTimeNanos;
    final long actualDelayNanos = TimeUnit.MILLISECONDS.toNanos(4);

    setActualDelay(actualDelayNanos);

    throttler.schedule(NO_OP, workTimeNanos);

    verifyRequestedDelay(expectedDelayNanos);
    assertEquals(expectedDelayNanos - actualDelayNanos, throttler.getPendingNanos());
  }

  @Test
  public void testClampDelayMillis() throws InterruptedException {
    final long maxDelayMillis = 10;
    final long maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(maxDelayMillis);

    ThrottlingScheduler throttler = new ThrottlingScheduler(maxDelayMillis, scheduledExecutorService, clock);
    throttler.setWorkFactor(0.5);

    setActualDelay(maxDelayNanos);

    // Note work time exceeds maxDelayMillis
    throttler.schedule(NO_OP, TimeUnit.MILLISECONDS.toNanos(100));

    verifyRequestedDelay(maxDelayNanos);
    assertEquals(0L, throttler.getPendingNanos());
  }

  @Test
  public void testDecreaseWorkFactor() {
    throttler.setWorkFactor(0.5);
    throttler.setPendingNanos(5000);

    throttler.setWorkFactor(0.3);
    assertEquals(5000, throttler.getPendingNanos());
  }

  @Test
  public void testOverflowOfDelayNanos() throws InterruptedException {
    throttler.setWorkFactor(0.5);
    throttler.setPendingNanos(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, throttler.getPendingNanos());

    // At a 50% work factor we'd expect work and delay to match. The function will try
    // to increment the pending delay nanos, which could (but should not) result in overflow.
    long workDurationNs = 5000;
    setActualDelay(workDurationNs);
    throttler.schedule(NO_OP, workDurationNs);
    verifyRequestedDelay(workDurationNs);

    // Expect delay nanos to be clamped during accumulation, and decreased by expected delay at the end.
    assertEquals(Long.MAX_VALUE - workDurationNs, throttler.getPendingNanos());
  }

  @Test
  public void testNegativePendingNanos() throws InterruptedException {
    throttler.setWorkFactor(0.5);
    throttler.setPendingNanos(-1000);
    assertEquals(-1000, throttler.getPendingNanos());

    // Note: we do not expect the delay time to be used because work time + pending delay is
    // negative.
    throttler.schedule(NO_OP, 500);

    // Should not be delayed with negative pending nanos
    Mockito.verify(scheduledExecutorService, Mockito.never())
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
    assertEquals(-1000 + 500, throttler.getPendingNanos());
  }

  @Test
  public void testNegativePendingNanosGoesPositive() throws InterruptedException {
    throttler.setWorkFactor(0.5);
    long startPendingNanos = -1000;
    throttler.setPendingNanos(startPendingNanos);
    assertEquals(-1000, throttler.getPendingNanos());

    setActualDelay(1250);

    // We request a delay greater than the starting pending delay.
    throttler.schedule(NO_OP, 1250);

    verifyRequestedDelay(1250);

    // Final pending delay should equal initial pending delay since we delay
    // for the exact requested amount.
    assertEquals(startPendingNanos, throttler.getPendingNanos());
  }

  private void setActualDelay(long actualDelayNs) {
    Mockito.when(clock.nanoTime()).thenReturn(0L).thenReturn(actualDelayNs);
  }

  private void verifyRequestedDelay(long expectedDelayTimeNanos) throws InterruptedException {
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(scheduledExecutorService)
        .schedule(runnableCaptor.capture(), Mockito.eq(expectedDelayTimeNanos), Mockito.eq(TimeUnit.NANOSECONDS));
    runnableCaptor.getValue().run();
  }
}
