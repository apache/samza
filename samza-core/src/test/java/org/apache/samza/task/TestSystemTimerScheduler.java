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

package org.apache.samza.task;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSystemTimerScheduler {

  private ScheduledExecutorService createExecutorService() {
    ScheduledExecutorService service = mock(ScheduledExecutorService.class);
    when(service.schedule((Runnable) anyObject(), anyLong(), anyObject())).thenAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        Runnable runnable = (Runnable) args[0];
        runnable.run();
        return mock(ScheduledFuture.class);
      });
    return service;
  }

  private void fireTimers(SystemTimerScheduler factory) {
    factory.removeReadyTimers().entrySet().forEach(entry -> {
        entry.getValue().onTimer(entry.getKey().getKey(), mock(MessageCollector.class), mock(TaskCoordinator.class));
      });
  }

  @Test
  public void testSingleTimer() {
    SystemTimerScheduler scheduler = SystemTimerScheduler.create(createExecutorService());
    List<String> results = new ArrayList<>();
    scheduler.setTimer("single-timer", 1, (key, collector, coordinator) -> {
        results.add(key);
      });

    fireTimers(scheduler);

    assertTrue(results.size() == 1);
    assertEquals(results.get(0), "single-timer");
  }

  @Test
  public void testMultipleTimers() {
    SystemTimerScheduler scheduler = SystemTimerScheduler.create(createExecutorService());
    List<String> results = new ArrayList<>();
    scheduler.setTimer("multiple-timer-3", 3, (key, collector, coordinator) -> {
        results.add(key + ":3");
      });
    scheduler.setTimer("multiple-timer-2", 2, (key, collector, coordinator) -> {
        results.add(key + ":2");
      });
    scheduler.setTimer("multiple-timer-1", 1, (key, collector, coordinator) -> {
        results.add(key + ":1");
      });

    fireTimers(scheduler);

    assertTrue(results.size() == 3);
    assertEquals(results.get(0), "multiple-timer-1:1");
    assertEquals(results.get(1), "multiple-timer-2:2");
    assertEquals(results.get(2), "multiple-timer-3:3");
  }

  @Test
  public void testMultipleKeys() {
    Object key1 = new Object();
    Object key2 = new Object();
    List<String> results = new ArrayList<>();

    SystemTimerScheduler scheduler = SystemTimerScheduler.create(createExecutorService());
    scheduler.setTimer(key1, 2, (key, collector, coordinator) -> {
        assertEquals(key, key1);
        results.add("key1:2");
      });
    scheduler.setTimer(key2, 1, (key, collector, coordinator) -> {
        assertEquals(key, key2);
        results.add("key2:1");
      });

    fireTimers(scheduler);

    assertTrue(results.size() == 2);
    assertEquals(results.get(0), "key2:1");
    assertEquals(results.get(1), "key1:2");
  }

  @Test
  public void testMultipleKeyTypes() {
    String key1 = "key";
    Long key2 = Long.MAX_VALUE;
    List<String> results = new ArrayList<>();

    SystemTimerScheduler scheduler = SystemTimerScheduler.create(createExecutorService());
    scheduler.setTimer(key1, 1, (key, collector, coordinator) -> {
        assertEquals(key, key1);
        results.add("key:1");
      });
    scheduler.setTimer(key2, 2, (key, collector, coordinator) -> {
        assertEquals(key.longValue(), Long.MAX_VALUE);
        results.add(Long.MAX_VALUE + ":2");
      });

    fireTimers(scheduler);

    assertTrue(results.size() == 2);
    assertEquals(results.get(0), key1 + ":1");
    assertEquals(results.get(1), key2 + ":2");
  }

  @Test
  public void testRemoveTimer() {
    ScheduledExecutorService service = mock(ScheduledExecutorService.class);
    ScheduledFuture future = mock(ScheduledFuture.class);
    when(future.cancel(anyBoolean())).thenReturn(true);
    when(service.schedule((Runnable) anyObject(), anyLong(), anyObject())).thenAnswer(invocation -> future);

    SystemTimerScheduler scheduler = SystemTimerScheduler.create(service);
    List<String> results = new ArrayList<>();
    scheduler.setTimer("timer", 1, (key, collector, coordinator) -> {
        results.add(key);
      });

    scheduler.deleteTimer("timer");

    fireTimers(scheduler);

    assertTrue(results.isEmpty());
    verify(future, times(1)).cancel(anyBoolean());
  }

  @Test
  public void testTimerListener() {
    SystemTimerScheduler scheduler = SystemTimerScheduler.create(createExecutorService());
    List<String> results = new ArrayList<>();
    scheduler.registerListener(() -> {
        results.add("timer-listener");
      });

    scheduler.setTimer("timer-listener", 1, (key, collector, coordinator) -> {
      });

    fireTimers(scheduler);

    assertTrue(results.size() == 1);
  }
}
