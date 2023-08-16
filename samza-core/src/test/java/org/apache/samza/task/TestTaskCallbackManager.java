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

import java.util.List;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.HighResolutionClock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestTaskCallbackManager {
  TaskCallbackManager callbackManager = null;
  TaskCallbackListener listener = null;

  @Before
  public void setup() {
    final TaskInstanceMetrics metrics = new TaskInstanceMetrics("Partition 0", new MetricsRegistryMap(), "");
    final long timeout = -1L;
    final int maxConcurrency = 2;
    final HighResolutionClock highResolutionClock = System::nanoTime;
    listener = new TaskCallbackListener() {
      @Override
      public void onComplete(TaskCallback callback) {
      }
      @Override
      public void onFailure(TaskCallback callback, Throwable t) {
      }
    };
    callbackManager = new TaskCallbackManager(listener, null, timeout, maxConcurrency, highResolutionClock);
  }

  @Test
  public void testCreateCallback() {
    TaskCallbackImpl callback = callbackManager.createCallback(new TaskName("Partition 0"), mock(IncomingMessageEnvelope.class), null);
    assertTrue(callback.matchSeqNum(0));

    callback = callbackManager.createCallback(new TaskName("Partition 0"), mock(IncomingMessageEnvelope.class), null);
    assertTrue(callback.matchSeqNum(1));
  }

  @Test
  public void testCreateDrainCallback() {
    TaskCallbackImpl callback = callbackManager.createCallback(new TaskName("Partition 0"), mock(IncomingMessageEnvelope.class), null, -1);
    assertTrue(callback.matchSeqNum(0));

    callback = callbackManager.createCallback(new TaskName("Partition 0"), mock(IncomingMessageEnvelope.class), null, -1);
    assertTrue(callback.matchSeqNum(1));
  }

  @Test
  public void testUpdateCallbackInOrder() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "topic", new Partition(0));
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callback0);
    assertEquals(1, callbacksToUpdate.size());
    TaskCallbackImpl callback = callbacksToUpdate.get(0);
    assertTrue(callback.matchSeqNum(0));
    assertEquals(ssp, callback.getSystemStreamPartition());
    assertEquals("0", callback.getOffset());

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator, 1, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback1);
    assertEquals(1, callbacksToUpdate.size());
    callback = callbacksToUpdate.get(0);
    assertTrue(callback.matchSeqNum(1));
    assertEquals(ssp, callback.getSystemStreamPartition());
    assertEquals("1", callback.getOffset());
  }

  @Test
  public void testUpdateCallbackOutofOrder() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "topic", new Partition(0));
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);

    // simulate out of order
    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(ssp, "2", null, null);
    TaskCallbackImpl callback2 = new TaskCallbackImpl(listener, taskName, envelope2, coordinator, 2, 0);
    List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callback2);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator, 1, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback1);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback0);
    assertEquals(3, callbacksToUpdate.size());
    TaskCallbackImpl callback = callbacksToUpdate.get(0);
    assertTrue(callback.matchSeqNum(0));
    assertEquals(ssp, callback.getSystemStreamPartition());
    assertEquals("0", callback.getOffset());

    callback = callbacksToUpdate.get(1);
    assertTrue(callback.matchSeqNum(1));
    assertEquals(ssp, callback.getSystemStreamPartition());
    assertEquals("1", callback.getOffset());

    callback = callbacksToUpdate.get(2);
    assertTrue(callback.matchSeqNum(2));
    assertEquals(ssp, callback.getSystemStreamPartition());
    assertEquals("2", callback.getOffset());
  }

  @Test
  public void testUpdateCallbackWithCoordinatorRequests() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "topic", new Partition(0));

    // simulate out of order
    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(ssp, "2", null, null);
    ReadableCoordinator coordinator2 = new ReadableCoordinator(taskName);
    coordinator2.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    TaskCallbackImpl callback2 = new TaskCallbackImpl(listener, taskName, envelope2, coordinator2, 2, 0);
    List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callback2);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    ReadableCoordinator coordinator1 = new ReadableCoordinator(taskName);
    coordinator1.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator1, 1, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback1);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback0);
    assertEquals(2, callbacksToUpdate.size());

    //Check for envelope0
    TaskCallbackImpl taskCallback = callbacksToUpdate.get(0);
    assertTrue(taskCallback.matchSeqNum(0));
    assertEquals(ssp, taskCallback.getSystemStreamPartition());
    assertEquals("0", taskCallback.getOffset());

    //Check for envelope1
    taskCallback = callbacksToUpdate.get(1);
    assertTrue(taskCallback.matchSeqNum(1));
    assertEquals(ssp, taskCallback.getSystemStreamPartition());
    assertEquals("1", taskCallback.getOffset());
  }

  @Test
  public void testUpdateShouldReturnAllCompletedCallbacksTillTheCommitRequestDefined() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp1 = new SystemStreamPartition("kafka", "topic", new Partition(0));
    SystemStreamPartition ssp2 = new SystemStreamPartition("kafka", "topic", new Partition(0));

    // Callback for Envelope3 contains commit request.
    IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(ssp2, "0", null, null);
    ReadableCoordinator coordinator3 = new ReadableCoordinator(taskName);
    coordinator3.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    TaskCallbackImpl callback3 = new TaskCallbackImpl(listener, taskName, envelope3, coordinator3, 3, 0);
    List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callback3);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(ssp1, "2", null, null);
    ReadableCoordinator coordinator2 = new ReadableCoordinator(taskName);
    coordinator2.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    TaskCallbackImpl callback2 = new TaskCallbackImpl(listener, taskName, envelope2, coordinator2, 2, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback2);
    assertTrue(callbacksToUpdate.isEmpty());

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp1, "1", null, null);
    ReadableCoordinator coordinator1 = new ReadableCoordinator(taskName);
    coordinator1.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator1, 1, 0);
    callbacksToUpdate = callbackManager.updateCallback(callback1);
    assertTrue(callbacksToUpdate.isEmpty());

    // Callback for Envelope0 contains commit request.
    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp1, "0", null, null);
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);

    // Check for both Envelope1, Envelope2, Envelope3 in callbacks to commit.
    // Two callbacks belonging to different system partition and has commitRequest defined is returned.
    callbacksToUpdate = callbackManager.updateCallback(callback0);
    assertEquals(2, callbacksToUpdate.size());
    TaskCallbackImpl callback = callbacksToUpdate.get(0);
    assertTrue(callback.matchSeqNum(0));
    assertEquals(envelope0.getSystemStreamPartition(), callback.getSystemStreamPartition());
    assertEquals(envelope0.getOffset(), callback.getOffset());

    callback = callbacksToUpdate.get(1);
    assertTrue(callback.matchSeqNum(1));
    assertEquals(envelope1.getSystemStreamPartition(), callback.getSystemStreamPartition());
    assertEquals(envelope1.getOffset(), callback.getOffset());
  }
}
