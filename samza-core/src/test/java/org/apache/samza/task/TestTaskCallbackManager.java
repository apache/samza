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

import org.apache.samza.Partition;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestTaskCallbackManager {
  TaskCallbackManager callbackManager = null;
  TaskCallbackListener listener = null;

  @Before
  public void setup() {
    TaskInstanceMetrics metrics = new TaskInstanceMetrics("Partition 0", new MetricsRegistryMap());
    listener = new TaskCallbackListener() {
      @Override
      public void onComplete(TaskCallback callback) {
      }
      @Override
      public void onFailure(TaskCallback callback, Throwable t) {
      }
    };
    callbackManager = new TaskCallbackManager(listener, null, -1, 2, () -> System.nanoTime());
  }

  @Test
  public void testCreateCallback() {
    TaskCallbackImpl callback = callbackManager.createCallback(new TaskName("Partition 0"), null, null);
    assertTrue(callback.matchSeqNum(0));

    callback = callbackManager.createCallback(new TaskName("Partition 0"), null, null);
    assertTrue(callback.matchSeqNum(1));
  }

  @Test
  public void testUpdateCallbackInOrder() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "topic", new Partition(0));
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    TaskCallbackImpl callbackToCommit = callbackManager.updateCallback(callback0);
    assertTrue(callbackToCommit.matchSeqNum(0));
    assertEquals(ssp, callbackToCommit.envelope.getSystemStreamPartition());
    assertEquals("0", callbackToCommit.envelope.getOffset());

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator, 1, 0);
    callbackToCommit = callbackManager.updateCallback(callback1);
    assertTrue(callbackToCommit.matchSeqNum(1));
    assertEquals(ssp, callbackToCommit.envelope.getSystemStreamPartition());
    assertEquals("1", callbackToCommit.envelope.getOffset());
  }

  @Test
  public void testUpdateCallbackOutofOrder() {
    TaskName taskName = new TaskName("Partition 0");
    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "topic", new Partition(0));
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);

    // simulate out of order
    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(ssp, "2", null, null);
    TaskCallbackImpl callback2 = new TaskCallbackImpl(listener, taskName, envelope2, coordinator, 2, 0);
    TaskCallbackImpl callbackToCommit = callbackManager.updateCallback(callback2);
    assertNull(callbackToCommit);

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator, 1, 0);
    callbackToCommit = callbackManager.updateCallback(callback1);
    assertNull(callbackToCommit);

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    callbackToCommit = callbackManager.updateCallback(callback0);
    assertTrue(callbackToCommit.matchSeqNum(2));
    assertEquals(ssp, callbackToCommit.envelope.getSystemStreamPartition());
    assertEquals("2", callbackToCommit.envelope.getOffset());
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
    TaskCallbackImpl callbackToCommit = callbackManager.updateCallback(callback2);
    assertNull(callbackToCommit);

    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(ssp, "1", null, null);
    ReadableCoordinator coordinator1 = new ReadableCoordinator(taskName);
    coordinator1.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    TaskCallbackImpl callback1 = new TaskCallbackImpl(listener, taskName, envelope1, coordinator1, 1, 0);
    callbackToCommit = callbackManager.updateCallback(callback1);
    assertNull(callbackToCommit);

    IncomingMessageEnvelope envelope0 = new IncomingMessageEnvelope(ssp, "0", null, null);
    ReadableCoordinator coordinator = new ReadableCoordinator(taskName);
    TaskCallbackImpl callback0 = new TaskCallbackImpl(listener, taskName, envelope0, coordinator, 0, 0);
    callbackToCommit = callbackManager.updateCallback(callback0);
    assertTrue(callbackToCommit.matchSeqNum(1));
    assertEquals(ssp, callbackToCommit.envelope.getSystemStreamPartition());
    assertEquals("1", callbackToCommit.envelope.getOffset());
    assertTrue(callbackToCommit.coordinator.requestedShutdownNow());
  }

}
