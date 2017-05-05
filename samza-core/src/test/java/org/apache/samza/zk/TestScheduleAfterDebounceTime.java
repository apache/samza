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

package org.apache.samza.zk;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestScheduleAfterDebounceTime {
  private static final long WAIT_TIME = 500;

  class TestObj {
    private volatile int i = 0;
    public void inc() {
      i++;
    }
    public void setTo(int val) {
      i = val;
    }
    public int get() {
      return i;
    }
  }

  @Test
  public void testSchedule() throws InterruptedException {
    ScheduleAfterDebounceTime scheduledQueue = new ScheduleAfterDebounceTime();
    final CountDownLatch latch = new CountDownLatch(1);

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    scheduledQueue.scheduleAfterDebounceTime("TEST1", WAIT_TIME, () -> {
        testObj.inc();
        latch.countDown();
      });
    // action is delayed
    Assert.assertEquals(0, testObj.get());

    boolean result = latch.await(WAIT_TIME * 2, TimeUnit.MILLISECONDS);
    Assert.assertTrue("Latch timed-out and task was not scheduled on time.", result);
    Assert.assertEquals(1, testObj.get());

    scheduledQueue.stopScheduler();
  }

  @Test
  public void testCancelAndSchedule() throws InterruptedException {
    ScheduleAfterDebounceTime scheduledQueue = new ScheduleAfterDebounceTime();
    final CountDownLatch test1Latch = new CountDownLatch(1);

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    scheduledQueue.scheduleAfterDebounceTime("TEST1", WAIT_TIME, testObj::inc);
    // next schedule should cancel the previous one with the same name
    scheduledQueue.scheduleAfterDebounceTime("TEST1", 2 * WAIT_TIME, () ->
      {
        testObj.inc();
        test1Latch.countDown();
      }
    );

    final TestObj testObj2 = new TestScheduleAfterDebounceTime.TestObj();
    // this schedule should not cancel the previous one, because it has different name
    scheduledQueue.scheduleAfterDebounceTime("TEST2", WAIT_TIME, testObj2::inc);

    boolean result = test1Latch.await(4 * WAIT_TIME, TimeUnit.MILLISECONDS);
    Assert.assertTrue("Latch timed-out. Scheduled tasks were not run correctly.", result);
    Assert.assertEquals(1, testObj.get());
    Assert.assertEquals(1, testObj2.get());

    scheduledQueue.stopScheduler();
  }

  @Test
  public void testRunnableWithExceptionInvokesCallback() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    ScheduleAfterDebounceTime scheduledQueue = new ScheduleAfterDebounceTime(e -> {
        Assert.assertEquals(RuntimeException.class, e.getClass());
        latch.countDown();
      });

    scheduledQueue.scheduleAfterDebounceTime("TEST1", WAIT_TIME, () ->
      {
        throw new RuntimeException("From the runnable!");
      });

    final TestObj testObj = new TestObj();
    scheduledQueue.scheduleAfterDebounceTime("TEST2", WAIT_TIME * 2, testObj::inc);

    boolean result = latch.await(5 * WAIT_TIME, TimeUnit.MILLISECONDS);
    Assert.assertTrue("Latch timed-out.", result);
    Assert.assertEquals(0, testObj.get());
  }
}
