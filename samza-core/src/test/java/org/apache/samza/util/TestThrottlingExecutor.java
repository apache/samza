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
import org.mockito.Mockito;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestThrottlingExecutor {
  private static final Runnable NO_OP = new Runnable() {
    @Override
    public void run() {
      // Do nothing.
    }
  };

  private HighResolutionClock clock;
  private ThrottlingExecutor executor;

  @Before
  public void setUp() {
    clock = Mockito.mock(HighResolutionClock.class);
    executor = new ThrottlingExecutor(clock);
  }

  @Test
  public void testInitialState() {
    ThrottlingExecutor throttler = new ThrottlingExecutor();
    assertEquals(0, throttler.getPendingNanos());
    assertEquals(1.0, throttler.getWorkFactor());
  }

  @Test
  public void testSetWorkRate() {
    executor.setWorkFactor(1.0);
    assertEquals(1.0, executor.getWorkFactor());

    executor.setWorkFactor(0.5);
    assertEquals(0.5, executor.getWorkFactor());

    executor.setWorkFactor(ThrottlingExecutor.MIN_WORK_FACTOR);
    assertEquals(ThrottlingExecutor.MIN_WORK_FACTOR, executor.getWorkFactor());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLessThan0PercentWorkRate() {
    new ThrottlingExecutor().setWorkFactor(-0.1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGreaterThan100PercentWorkRate() {
    new ThrottlingExecutor().setWorkFactor(1.1);
  }

  @Test
  public void test100PercentWorkRate() throws InterruptedException {
    setWorkTime(TimeUnit.MILLISECONDS.toNanos(5));

    executor.execute(NO_OP);

    assertEquals(0L, executor.getPendingNanos());

    // At 100% work rate sleep should not be called
    Mockito.verify(clock, Mockito.never()).sleep(Mockito.anyLong());
  }

  @Test
  public void test50PercentWorkRate() throws InterruptedException {
    executor.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    setWorkTime(workTimeNanos);
    // Sleep time is same as work time at 50% work rate
    setExpectedAndActualSleepTime(workTimeNanos, workTimeNanos);
    executor.execute(NO_OP);

    verifySleepTime(workTimeNanos);
    assertEquals(0L, executor.getPendingNanos());
  }

  @Test
  public void testMinWorkRate() throws InterruptedException {
    final double workFactor = ThrottlingExecutor.MIN_WORK_FACTOR;
    executor.setWorkFactor(workFactor);

    // The math to work out how much to multiply work time to get expected delay time
    double workToDelayFactor = (1.0 - workFactor) / workFactor;

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long delayTimeNanos = (long) (workToDelayFactor * workTimeNanos);

    setWorkTime(workTimeNanos);
    setExpectedAndActualSleepTime(delayTimeNanos, delayTimeNanos);

    executor.execute(NO_OP);

    verifySleepTime(delayTimeNanos);
    assertEquals(0, executor.getPendingNanos());
  }

  @Test
  public void testSleepOvershoot() throws InterruptedException {
    executor.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long expectedDelayNanos = workTimeNanos;
    final long actualDelayTimeNanos = TimeUnit.MILLISECONDS.toNanos(6);

    setWorkTime(workTimeNanos);
    setExpectedAndActualSleepTime(expectedDelayNanos, actualDelayTimeNanos);

    executor.execute(NO_OP);

    verifySleepTime(expectedDelayNanos);
    assertEquals(expectedDelayNanos - actualDelayTimeNanos, executor.getPendingNanos());
  }

  @Test
  public void testSleepUndershoot() throws InterruptedException {
    executor.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long expectedDelayNanos = workTimeNanos;
    final long actualDelayNanos = TimeUnit.MILLISECONDS.toNanos(4);

    setWorkTime(workTimeNanos);
    setExpectedAndActualSleepTime(expectedDelayNanos, actualDelayNanos);

    executor.execute(NO_OP);

    verifySleepTime(expectedDelayNanos);
    assertEquals(expectedDelayNanos - actualDelayNanos, executor.getPendingNanos());
  }

  @Test
  public void testApplyPendingSleepNanos() throws InterruptedException {
    // This verifies that the executor tries to re-apply pending sleep time on the next execution.
    executor.setWorkFactor(0.5);

    final long workTimeNanos = TimeUnit.MILLISECONDS.toNanos(5);
    final long actualDelayNanos1 = TimeUnit.MILLISECONDS.toNanos(4);
    final long actualDelayNanos2 = TimeUnit.MILLISECONDS.toNanos(6);

    // First execution
    setWorkTime(workTimeNanos);
    setExpectedAndActualSleepTime(workTimeNanos, actualDelayNanos1);

    executor.execute(NO_OP);

    verifySleepTime(workTimeNanos);
    assertEquals(workTimeNanos - actualDelayNanos1, executor.getPendingNanos());


    // Second execution
    setWorkTime(workTimeNanos);
    setExpectedAndActualSleepTime(workTimeNanos, actualDelayNanos2);

    executor.execute(NO_OP);

    verifySleepTime(workTimeNanos);
    assertEquals(0L, executor.getPendingNanos());
  }

  @Test
  public void testDecreaseWorkFactor() {
    executor.setWorkFactor(0.5);
    executor.setPendingNanos(5000);

    executor.setWorkFactor(0.3);
    assertEquals(5000, executor.getPendingNanos());
  }

  @Test
  public void testOverflowOfSleepNanos() throws InterruptedException {
    executor.setWorkFactor(0.5);
    executor.setPendingNanos(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, executor.getPendingNanos());

    // At a 50% work factor we'd expect work and sleep to match. As they don't, the function will
    // try to increment the pending sleep nanos, which could (but should not) result in overflow.
    setWorkTime(5000);

    executor.execute(NO_OP);

    // Expect sleep nanos to be clamped to the maximum long value
    verifySleepTime(Long.MAX_VALUE);
  }

  @Test
  public void testNegativePendingNanos() throws InterruptedException {
    executor.setWorkFactor(0.5);
    executor.setPendingNanos(-1000);
    assertEquals(-1000, executor.getPendingNanos());

    // Note: we do not expect the delay time to be used because work time + pending delay is
    // negative.
    setWorkTime(500);

    executor.execute(NO_OP);

    // Sleep should not be called with negative pending nanos
    Mockito.verify(clock, Mockito.never()).sleep(Mockito.anyLong());
    assertEquals(-1000 + 500, executor.getPendingNanos());
  }

  @Test
  public void testNegativePendingNanosGoesPositive() throws InterruptedException {
    executor.setWorkFactor(0.5);
    long startPendingNanos = -1000;
    executor.setPendingNanos(startPendingNanos);
    assertEquals(-1000, executor.getPendingNanos());

    setWorkTime(1250);

    executor.execute(NO_OP);

    verifySleepTime(1250 + startPendingNanos);
    assertEquals(0, executor.getPendingNanos());
  }

  private void setWorkTime(long workTimeNanos) {
    Mockito.when(clock.nanoTime()).thenReturn(0L).thenReturn(workTimeNanos);
  }

  private void setExpectedAndActualSleepTime(long expectedDelayTimeNanos, long actualDelayTimeNanos) throws InterruptedException {
    Mockito.when(clock.sleep(expectedDelayTimeNanos))
        .thenReturn(expectedDelayTimeNanos - actualDelayTimeNanos);
  }

  private void verifySleepTime(long expectedDelayTimeNanos) throws InterruptedException {
    Mockito.verify(clock).sleep(expectedDelayTimeNanos);
  }
}
