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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestContainerHeartbeatMonitor {
  @Mock
  private Runnable onExpired;
  @Mock
  private ContainerHeartbeatClient containerHeartbeatClient;

  private ScheduledExecutorService scheduler;
  /**
   * Use this to detect when the scheduler has finished executing the fixed-rate task.
   */
  private CountDownLatch schedulerFixedRateExecutionLatch;

  private ContainerHeartbeatMonitor containerHeartbeatMonitor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.schedulerFixedRateExecutionLatch = new CountDownLatch(1);
    this.scheduler = buildScheduledExecutorService(this.schedulerFixedRateExecutionLatch);
    this.containerHeartbeatMonitor =
        new ContainerHeartbeatMonitor(this.onExpired, this.containerHeartbeatClient, this.scheduler);
  }

  @Test
  public void testCallbackWhenHeartbeatDead() throws InterruptedException {
    ContainerHeartbeatResponse response = new ContainerHeartbeatResponse(false);
    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(response);
    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(2, TimeUnit.SECONDS);
    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    // check that the shutdown task got submitted, but don't actually execute it since it will shut down the process
    verify(this.scheduler).schedule(any(Runnable.class), eq((long) ContainerHeartbeatMonitor.SHUTDOWN_TIMOUT_MS),
        eq(TimeUnit.MILLISECONDS));
    verify(this.onExpired).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  @Test
  public void testDoesNotCallbackWhenHeartbeatAlive() throws InterruptedException {
    ContainerHeartbeatResponse response = new ContainerHeartbeatResponse(true);
    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(response);
    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(2, TimeUnit.SECONDS);
    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    // shutdown task should not have been submitted
    verify(this.scheduler, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(this.onExpired, never()).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  /**
   * Build a mock {@link ScheduledExecutorService} which will execute a fixed-rate task once. It will count down on
   * {@code schedulerFixedRateExecutionLatch} when the task is finished executing.
   * It will not execute any one-shot tasks, but it can be used to verify that the one-shot task was submitted.
   */
  private static ScheduledExecutorService buildScheduledExecutorService(
      CountDownLatch schedulerFixedRateExecutionLatch) {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.scheduleAtFixedRate(any(), eq(0L), eq((long) ContainerHeartbeatMonitor.SCHEDULE_MS),
        eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
            Runnable command = invocation.getArgumentAt(0, Runnable.class);
            (new Thread(() -> {
                // just need to invoke the command once for these tests
                command.run();
                // notify that the execution is done, so verifications can begin
                schedulerFixedRateExecutionLatch.countDown();
              })).start();
            // return value is not used by ContainerHeartbeatMonitor
            return null;
          });
    return scheduler;
  }
}
