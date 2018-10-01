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
package org.apache.samza.scheduler;

import org.apache.samza.task.EpochTimeScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class TestCallbackSchedulerImpl {
  @Mock
  private EpochTimeScheduler epochTimeScheduler;

  private CallbackSchedulerImpl scheduler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    scheduler = new CallbackSchedulerImpl(epochTimeScheduler);
  }

  /**
   * scheduleCallback should delegate to the inner scheduler
   */
  @Test
  public void testScheduleCallback() {
    @SuppressWarnings("unchecked")
    ScheduledCallback<String> stringCallback = mock(ScheduledCallback.class);
    scheduler.scheduleCallback("string_key", 123, stringCallback);
    verify(epochTimeScheduler).setTimer("string_key", 123, stringCallback);

    // check some other type of key
    @SuppressWarnings("unchecked")
    ScheduledCallback<Integer> intCallback = mock(ScheduledCallback.class);
    scheduler.scheduleCallback(777, 456, intCallback);
    verify(epochTimeScheduler).setTimer(777, 456, intCallback);
  }

  /**
   * deleteCallback should delegate to the inner scheduler
   */
  @Test
  public void testDeleteCallback() {
    scheduler.deleteCallback("string_key");
    verify(epochTimeScheduler).deleteTimer("string_key");

    // check some other type of key
    scheduler.deleteCallback(777);
    verify(epochTimeScheduler).deleteTimer(777);
  }
}