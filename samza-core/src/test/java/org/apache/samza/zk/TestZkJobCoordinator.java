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

import java.util.HashMap;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.zk.ZkJobCoordinator.ZkSessionStateChangedListener;
import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class TestZkJobCoordinator {
  private static final String TEST_BARRIER_ROOT = "/testBarrierRoot";
  private static final String TEST_JOB_MODEL_VERSION = "1";

  @Test
  public void testFollowerShouldStopWhenNotPartOfGeneratedJobModel() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(new MapConfig(), new NoOpMetricsRegistry(), zkUtils));
    zkJobCoordinator.onNewJobModelAvailable(TEST_JOB_MODEL_VERSION);

    verify(zkJobCoordinator, Mockito.atMost(1)).stop();
  }

  @Test
  public void testShouldRemoveBufferedEventsInDebounceQueueOnSessionExpiration() {
    ZkKeyBuilder keyBuilder = Mockito.mock(ZkKeyBuilder.class);
    ZkClient mockZkClient = Mockito.mock(ZkClient.class);
    when(keyBuilder.getJobModelVersionBarrierPrefix()).thenReturn(TEST_BARRIER_ROOT);

    ZkUtils zkUtils = Mockito.mock(ZkUtils.class);
    when(zkUtils.getKeyBuilder()).thenReturn(keyBuilder);
    when(zkUtils.getZkClient()).thenReturn(mockZkClient);
    when(zkUtils.getJobModel(TEST_JOB_MODEL_VERSION)).thenReturn(new JobModel(new MapConfig(), new HashMap<>()));

    ScheduleAfterDebounceTime mockDebounceTimer = Mockito.mock(ScheduleAfterDebounceTime.class);

    ZkJobCoordinator zkJobCoordinator = Mockito.spy(new ZkJobCoordinator(new MapConfig(), new NoOpMetricsRegistry(), zkUtils));
    zkJobCoordinator.debounceTimer = mockDebounceTimer;
    final ZkSessionStateChangedListener zkSessionStateChangedListener = zkJobCoordinator.new ZkSessionStateChangedListener();

    zkSessionStateChangedListener.handleStateChanged(Watcher.Event.KeeperState.Expired);

    verify(zkUtils).incGeneration();
    verify(mockDebounceTimer).cancelAllScheduledActions();
    verify(mockDebounceTimer).scheduleAfterDebounceTime(Mockito.eq("ZK_SESSION_ERROR"), Mockito.eq(0L), Mockito.any(Runnable.class));
  }
}
