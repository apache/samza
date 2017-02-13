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
import org.junit.Before;
import org.junit.Test;


public class TestScheduleAfterDebounceTime {
  private static final long DEBOUNCE_TIME = 500;
  int i = 0;
  @Before
  public void setup() {

  }

  class TestObj {
    public void inc() {
      i++;
    }
    public void setTo(int val) {
      i=val;
    }
    public void doNothing() {

    }
  }
  @Test
  public void testSchedule() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    debounceTimer.scheduleAfterDebounceTime("TEST1", DEBOUNCE_TIME, () -> {
      testObj.inc();
    });
    // action is delayed
    Assert.assertEquals(0, i);

    try { Thread.sleep(DEBOUNCE_TIME + 10); } catch (InterruptedException e) {Assert.fail("Sleep was interrupted");}

    // debounce time passed
    Assert.assertEquals(1, i);
  }

  @Test
  public void testCancelAndSchedule() {
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();

    final TestObj testObj = new TestScheduleAfterDebounceTime.TestObj();
    debounceTimer.scheduleAfterDebounceTime("TEST1", DEBOUNCE_TIME, () -> {
      testObj.inc();
    });
    Assert.assertEquals(0, i);

    // next schedule should cancel the previous one with the same name
    debounceTimer.scheduleAfterDebounceTime("TEST1", 2*DEBOUNCE_TIME, () -> {
      testObj.setTo(100);
    });

    try { Thread.sleep(DEBOUNCE_TIME + 10); } catch (InterruptedException e) {Assert.fail("Sleep was interrupted");}
    // still the old value
    Assert.assertEquals(0, i);

    // this schedule should not cancel the previous one, because it has different name
    debounceTimer.scheduleAfterDebounceTime("TEST2", DEBOUNCE_TIME, () -> {
      testObj.doNothing();
    });

    try { Thread.sleep(3*DEBOUNCE_TIME + 10); } catch (InterruptedException e) {Assert.fail("Sleep was interrupted");}
    Assert.assertEquals(100, i);
  }
}
