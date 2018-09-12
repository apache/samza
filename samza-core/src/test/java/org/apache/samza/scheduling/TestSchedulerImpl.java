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
package org.apache.samza.scheduling;

import org.apache.samza.task.SystemTimerScheduler;
import org.apache.samza.task.TimerCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;


public class TestSchedulerImpl {
  @Mock
  private SystemTimerScheduler systemTimerScheduler;

  private SchedulerImpl scheduler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    scheduler = new SchedulerImpl(systemTimerScheduler);
  }

  /**
   * scheduleCallback should delegate to the inner scheduler
   */
  @Test
  public void testScheduleCallback() {
    @SuppressWarnings("unchecked")
    TimerCallback<String> stringCallback = mock(TimerCallback.class);
    scheduler.scheduleCallback("string_key", 123, stringCallback);
    verify(systemTimerScheduler).setTimer("string_key", 123, stringCallback);

    // check some other type of key
    @SuppressWarnings("unchecked")
    TimerCallback<Integer> intCallback = mock(TimerCallback.class);
    scheduler.scheduleCallback(777, 456, intCallback);
    verify(systemTimerScheduler).setTimer(777, 456, intCallback);
  }

  /**
   * deleteCallback should delegate to the inner scheduler
   */
  @Test
  public void testDeleteCallback() {
    scheduler.deleteCallback("string_key");
    verify(systemTimerScheduler).deleteTimer("string_key");

    // check some other type of key
    scheduler.deleteCallback(777);
    verify(systemTimerScheduler).deleteTimer(777);
  }
}