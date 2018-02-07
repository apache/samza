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

public class TestSystemTimerSchedulerFactory {

  private ScheduledExecutorService createExecutorService() {
    ScheduledExecutorService service = mock(ScheduledExecutorService.class);
    when(service.schedule((Runnable) anyObject(), anyLong(), anyObject())).thenAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        Runnable runnable = (Runnable) args[0];
        runnable.run();
        return null;
      });
    return service;
  }

  private void fireTimers(SystemTimerSchedulerFactory factory) {
    factory.removeReadyTimers().entrySet().forEach(entry -> {
        entry.getValue().onTimer(entry.getKey().getKey(), mock(MessageCollector.class), mock(TaskCoordinator.class));
      });
  }

  @Test
  public void testSingleTimer() {
    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(createExecutorService());
    SystemTimerSchedulerFactory.SystemTimerScheduler<String> scheduler = factory.getScheduler("single-timer");
    List<String> results = new ArrayList<>();
    scheduler.schedule(1, (key, collector, coordinator) -> {
        results.add(key);
      });

    fireTimers(factory);

    assertTrue(results.size() == 1);
    assertEquals(results.get(0), "single-timer");
  }

  @Test
  public void testMultipleTimers() {
    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(createExecutorService());
    SystemTimerSchedulerFactory.SystemTimerScheduler<String> scheduler = factory.getScheduler("multiple-timer");
    List<String> results = new ArrayList<>();
    scheduler.schedule(3, (key, collector, coordinator) -> {
        results.add(key + ":3");
      });
    scheduler.schedule(2, (key, collector, coordinator) -> {
        results.add(key + ":2");
      });
    scheduler.schedule(1, (key, collector, coordinator) -> {
        results.add(key + ":1");
      });

    fireTimers(factory);

    assertTrue(results.size() == 3);
    assertEquals(results.get(0), "multiple-timer:1");
    assertEquals(results.get(1), "multiple-timer:2");
    assertEquals(results.get(2), "multiple-timer:3");
  }

  @Test
  public void testMultipleKeys() {
    Object key1 = new Object();
    Object key2 = new Object();
    List<String> results = new ArrayList<>();

    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(createExecutorService());
    SystemTimerSchedulerFactory.SystemTimerScheduler<Object> scheduler = factory.getScheduler(key1);
    scheduler.schedule(2, (key, collector, coordinator) -> {
        assertEquals(key, key1);
        results.add("key1:2");
      });
    scheduler = factory.getScheduler(key2);
    scheduler.schedule(1, (key, collector, coordinator) -> {
        assertEquals(key, key2);
        results.add("key2:1");
      });

    fireTimers(factory);

    assertTrue(results.size() == 2);
    assertEquals(results.get(0), "key2:1");
    assertEquals(results.get(1), "key1:2");
  }

  @Test
  public void testMultipleKeyTypes() {
    String key1 = "key";
    Long key2 = Long.MAX_VALUE;
    List<String> results = new ArrayList<>();

    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(createExecutorService());
    SystemTimerSchedulerFactory.SystemTimerScheduler<String> scheduler = factory.getScheduler(key1);
    scheduler.schedule(1, (key, collector, coordinator) -> {
        assertEquals(key, key1);
        results.add("key:1");
      });
    SystemTimerSchedulerFactory.SystemTimerScheduler<Long> scheduler2 = factory.getScheduler(key2);
    scheduler2.schedule(2, (key, collector, coordinator) -> {
        assertEquals(key.longValue(), Long.MAX_VALUE);
        results.add(Long.MAX_VALUE + ":2");
      });

    fireTimers(factory);

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

    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(service);
    SystemTimerSchedulerFactory.SystemTimerScheduler<String> scheduler = factory.getScheduler("timer");
    List<String> results = new ArrayList<>();
    scheduler.schedule(1, (key, collector, coordinator) -> {
        results.add(key);
      });

    factory.removeScheduler("timer");

    fireTimers(factory);

    assertTrue(results.isEmpty());
    verify(future, times(1)).cancel(anyBoolean());
  }

  @Test
  public void testTimerListener() {
    SystemTimerSchedulerFactory factory = SystemTimerSchedulerFactory.create(createExecutorService());
    SystemTimerSchedulerFactory.SystemTimerScheduler<String> scheduler = factory.getScheduler("timer-listener");
    List<String> results = new ArrayList<>();
    factory.registerListener(() -> {
        results.add("timer-listener");
      });

    scheduler.schedule(1, (key, collector, coordinator) -> {
      });

    fireTimers(factory);

    assertTrue(results.size() == 1);
  }
}
