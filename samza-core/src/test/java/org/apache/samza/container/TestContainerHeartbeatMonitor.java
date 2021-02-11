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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.CoordinationConstants;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.metadatastore.MetadataStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestContainerHeartbeatMonitor {
  private static final String COORDINATOR_URL = "http://some-host.prod.linkedin.com";
  private static final ContainerHeartbeatResponse FAILURE_RESPONSE = new ContainerHeartbeatResponse(false);
  private static final ContainerHeartbeatResponse SUCCESS_RESPONSE = new ContainerHeartbeatResponse(true);
  private static final String CONTAINER_EXECUTION_ID = "0";

  @Mock
  private Runnable onExpired;
  @Mock
  private ContainerHeartbeatClient containerHeartbeatClient;
  @Mock
  private MetadataStore coordinatorStreamStore;

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
    this.containerHeartbeatMonitor = buildContainerHeartbeatMonitor(false);
  }

  private ContainerHeartbeatMonitor buildContainerHeartbeatMonitor(boolean enableAMHighAvailability) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.YARN_AM_HIGH_AVAILABILITY_ENABLED, String.valueOf(enableAMHighAvailability));
    configMap.put(JobConfig.YARN_CONTAINER_HEARTBEAT_RETRY_COUNT, "5");
    configMap.put(JobConfig.YARN_CONTAINER_HEARTBEAT_RETRY_SLEEP_DURATION_MS, "10");

    return new ContainerHeartbeatMonitor(this.onExpired, this.containerHeartbeatClient, this.scheduler, COORDINATOR_URL,
        CONTAINER_EXECUTION_ID, coordinatorStreamStore, new MapConfig(configMap));
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
    assertEquals("Heartbeat expired count should be 1", 1,
        containerHeartbeatMonitor.getMetrics().getHeartbeatExpiredCount().getCount());
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
    assertEquals("Heartbeat expired count should be 0", 0,
        containerHeartbeatMonitor.getMetrics().getHeartbeatExpiredCount().getCount());
    // shutdown task should not have been submitted
    verify(this.scheduler, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(this.onExpired, never()).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  @Test
  public void testReestablishConnectionWithNewAM() throws InterruptedException {
    String newCoordinatorUrl = "http://some-host-2.prod.linkedin.com";
    this.containerHeartbeatMonitor = spy(buildContainerHeartbeatMonitor(true));
    CoordinatorStreamValueSerde serde = new CoordinatorStreamValueSerde(SetConfig.TYPE);
    ContainerHeartbeatMonitor.ContainerHeartbeatMetrics metrics = this.containerHeartbeatMonitor.getMetrics();

    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(FAILURE_RESPONSE).thenReturn(SUCCESS_RESPONSE);
    when(this.containerHeartbeatMonitor
        .createContainerHeartbeatClient(newCoordinatorUrl, CONTAINER_EXECUTION_ID))
        .thenReturn(this.containerHeartbeatClient);
    when(this.coordinatorStreamStore.get(CoordinationConstants.YARN_COORDINATOR_URL))
        .thenReturn(serde.toBytes(newCoordinatorUrl));

    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(2, TimeUnit.SECONDS);
    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    assertEquals("Heartbeat expired count should be 1", 1, metrics.getHeartbeatExpiredCount().getCount());
    assertEquals("Heartbeat established failure count should be 0", 0,
        metrics.getHeartbeatEstablishedFailureCount().getCount());
    assertEquals("Heartbeat established with new AM should be 1", 1,
        metrics.getHeartbeatEstablishedWithNewAmCount().getCount());
    // shutdown task should not have been submitted
    verify(this.scheduler, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(this.onExpired, never()).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  @Test
  public void testFailedToFetchNewAMCoordinatorUrl() throws InterruptedException {
    this.containerHeartbeatMonitor = spy(buildContainerHeartbeatMonitor(true));
    CoordinatorStreamValueSerde serde = new CoordinatorStreamValueSerde(SetConfig.TYPE);
    ContainerHeartbeatMonitor.ContainerHeartbeatMetrics metrics = this.containerHeartbeatMonitor.getMetrics();

    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(FAILURE_RESPONSE);
    when(this.coordinatorStreamStore.get(CoordinationConstants.YARN_COORDINATOR_URL))
        .thenReturn(serde.toBytes(COORDINATOR_URL));

    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(2, TimeUnit.SECONDS);

    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    assertEquals("Heartbeat expired count should be 1", 1, metrics.getHeartbeatExpiredCount().getCount());
    assertEquals("Heartbeat established failure count should be 1", 1,
        metrics.getHeartbeatEstablishedFailureCount().getCount());
    // shutdown task should have been submitted
    verify(this.scheduler).schedule(any(Runnable.class), eq((long) ContainerHeartbeatMonitor.SHUTDOWN_TIMOUT_MS),
        eq(TimeUnit.MILLISECONDS));
    verify(this.onExpired).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  @Test
  public void testConnectToNewAMFailed() throws InterruptedException {
    String newCoordinatorUrl = "http://some-host-2.prod.linkedin.com";
    this.containerHeartbeatMonitor = spy(buildContainerHeartbeatMonitor(true));
    CoordinatorStreamValueSerde serde = new CoordinatorStreamValueSerde(SetConfig.TYPE);
    ContainerHeartbeatMonitor.ContainerHeartbeatMetrics metrics = this.containerHeartbeatMonitor.getMetrics();

    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(FAILURE_RESPONSE);
    when(this.containerHeartbeatMonitor.createContainerHeartbeatClient(newCoordinatorUrl, CONTAINER_EXECUTION_ID))
        .thenReturn(this.containerHeartbeatClient);
    when(this.coordinatorStreamStore.get(CoordinationConstants.YARN_COORDINATOR_URL))
        .thenReturn(serde.toBytes(newCoordinatorUrl));

    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(2, TimeUnit.SECONDS);

    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    assertEquals("Heartbeat expired count should be 1", 1, metrics.getHeartbeatExpiredCount().getCount());
    assertEquals("Heartbeat established failure count should be 1", 1,
        metrics.getHeartbeatEstablishedFailureCount().getCount());
    // shutdown task should have been submitted
    verify(this.scheduler).schedule(any(Runnable.class), eq((long) ContainerHeartbeatMonitor.SHUTDOWN_TIMOUT_MS),
        eq(TimeUnit.MILLISECONDS));
    verify(this.onExpired).run();

    this.containerHeartbeatMonitor.stop();
    verify(this.scheduler).shutdown();
  }

  @Test
  public void testConnectToNewAMSerdeException() throws InterruptedException {
    String newCoordinatorUrl = "http://some-host-2.prod.linkedin.com";
    this.containerHeartbeatMonitor = spy(buildContainerHeartbeatMonitor(true));
    CoordinatorStreamValueSerde serde = new CoordinatorStreamValueSerde(SetConfig.TYPE);
    ContainerHeartbeatMonitor.ContainerHeartbeatMetrics metrics = this.containerHeartbeatMonitor.getMetrics();

    when(this.containerHeartbeatClient.requestHeartbeat()).thenReturn(FAILURE_RESPONSE);
    when(this.containerHeartbeatMonitor.createContainerHeartbeatClient(newCoordinatorUrl, CONTAINER_EXECUTION_ID))
        .thenReturn(this.containerHeartbeatClient);
    when(this.coordinatorStreamStore.get(CoordinationConstants.YARN_COORDINATOR_URL))
        .thenThrow(new NullPointerException("serde failed"));

    this.containerHeartbeatMonitor.start();
    // wait for the executor to finish the heartbeat check task
    boolean fixedRateTaskCompleted = this.schedulerFixedRateExecutionLatch.await(10, TimeUnit.SECONDS);

    assertTrue("Did not complete heartbeat check", fixedRateTaskCompleted);
    assertEquals("Heartbeat expired count should be 1", 1, metrics.getHeartbeatExpiredCount().getCount());
    assertEquals("Heartbeat established failure count should be 1", 1,
        metrics.getHeartbeatEstablishedFailureCount().getCount());
    // shutdown task should have been submitted
    verify(this.scheduler).schedule(any(Runnable.class), eq((long) ContainerHeartbeatMonitor.SHUTDOWN_TIMOUT_MS),
        eq(TimeUnit.MILLISECONDS));
    verify(this.onExpired).run();

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
