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

package org.apache.samza.container;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestContainerHeartbeatMonitor {
  @Test
  public void testCallbackWhenHeartbeatDead() throws InterruptedException {
    ContainerHeartbeatClient mockClient = mock(ContainerHeartbeatClient.class);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Runnable onExpired = countDownLatch::countDown;
    ContainerHeartbeatResponse response = new ContainerHeartbeatResponse(false);
    when(mockClient.requestHeartbeat()).thenReturn(response);
    ScheduledExecutorService scheduler = buildScheduledExecutorService();
    ContainerHeartbeatMonitor monitor = new ContainerHeartbeatMonitor(onExpired, mockClient, scheduler);
    monitor.start();
    boolean success = countDownLatch.await(2, TimeUnit.SECONDS);
    assertTrue(success);
    // check that the shutdown task got submitted, but don't actually execute it since it will shut down the process
    verify(scheduler).schedule(any(Runnable.class), eq((long) ContainerHeartbeatMonitor.SHUTDOWN_TIMOUT_MS),
        eq(TimeUnit.MILLISECONDS));

    monitor.stop();
    verify(scheduler).shutdown();
  }

  @Test
  public void testDoesNotCallbackWhenHeartbeatAlive() throws InterruptedException {
    ContainerHeartbeatClient client = mock(ContainerHeartbeatClient.class);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Runnable onExpired = countDownLatch::countDown;
    ContainerHeartbeatResponse response = new ContainerHeartbeatResponse(true);
    when(client.requestHeartbeat()).thenReturn(response);
    ScheduledExecutorService scheduler = buildScheduledExecutorService();
    ContainerHeartbeatMonitor monitor = new ContainerHeartbeatMonitor(onExpired, client, scheduler);
    monitor.start();
    boolean success = countDownLatch.await(2, TimeUnit.SECONDS);
    assertFalse(success);
    assertEquals(1, countDownLatch.getCount());
    // shutdown task should not have been submitted
    verify(scheduler, never()).schedule(any(Runnable.class), anyLong(), any());

    monitor.stop();
    verify(scheduler).shutdown();
  }

  /**
   * Build a mock {@link ScheduledExecutorService} which will execute a fixed-rate task once. It will not execute any
   * one-shot tasks, but it can be used to verify that the one-shot task was submitted.
   */
  private static ScheduledExecutorService buildScheduledExecutorService() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.scheduleAtFixedRate(any(), eq(0L), eq((long) ContainerHeartbeatMonitor.SCHEDULE_MS),
        eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
            Runnable command = invocation.getArgumentAt(0, Runnable.class);
            // just need to invoke the command once for these tests
            (new Thread(command)).start();
            // return value is not used by ContainerHeartbeatMonitor
            return null;
          });
    return scheduler;
  }
}
