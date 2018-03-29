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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.util.HighResolutionClock;
import org.apache.samza.util.Util;


/**
 * TaskCallbackManager manages the life cycle of {@link AsyncStreamTask} callbacks,
 * including creation, update and status. Internally it maintains a PriorityQueue
 * for the callbacks based on the sequence number, and updates the offsets for checkpointing
 * by always moving forward to the latest contiguous callback (uses the high watermark).
 */
class TaskCallbackManager {

  private static final class TaskCallbacks {
    private final Queue<TaskCallbackImpl> callbacks = new PriorityQueue<>();
    private final Object lock = new Object();
    private long nextSeqNum = 0L;

    /**
     * Adding the newly complete callback to the callback queue
     * Move the queue to the last contiguous callback to commit offset
     * @param cb new callback completed
     * @return list of callbacks to be committed
     */
    List<TaskCallbackImpl> update(TaskCallbackImpl cb) {
      synchronized (lock) {
        callbacks.add(cb);
        List<TaskCallbackImpl> callbacksToUpdate = new ArrayList<>();
        // look for the last contiguous callback
        while (!callbacks.isEmpty() && callbacks.peek().matchSeqNum(nextSeqNum)) {
          ++nextSeqNum;
          TaskCallbackImpl callback = callbacks.poll();
          callbacksToUpdate.add(callback);
          if (callback.coordinator.commitRequest().isDefined()) {
            break;
          }
        }
        return callbacksToUpdate;
      }
    }
  }

  private long seqNum = 0L;
  private final TaskCallbacks completedCallbacks = new TaskCallbacks();
  private final ScheduledExecutorService timer;
  private final TaskCallbackListener listener;
  private final long timeout;
  private final int maxConcurrency;
  private final HighResolutionClock clock;

  public TaskCallbackManager(TaskCallbackListener listener,
      ScheduledExecutorService timer,
      long timeout,
      int maxConcurrency,
      HighResolutionClock clock) {
    this.listener = listener;
    this.timer = timer;
    this.timeout = timeout;
    this.maxConcurrency = maxConcurrency;
    this.clock = clock;
  }

  public TaskCallbackImpl createCallback(TaskName taskName,
      IncomingMessageEnvelope envelope,
      ReadableCoordinator coordinator) {
    final TaskCallbackImpl callback =
        new TaskCallbackImpl(listener, taskName, envelope, coordinator, seqNum++, clock.nanoTime());
    if (timer != null) {
      Runnable timerTask = new Runnable() {
        @Override
        public void run() {
          Util.printThreadDump("Thread dump at task callback timeout");
          String msg = "Callback for task {} " + callback.taskName + " timed out after " + timeout + " ms.";
          callback.failure(new SamzaException(msg));
        }
      };
      ScheduledFuture scheduledFuture = timer.schedule(timerTask, timeout, TimeUnit.MILLISECONDS);
      callback.setScheduledFuture(scheduledFuture);
    }

    return callback;
  }

  /**
   * Update the task callbacks with the new callback completed.
   * It uses a high-watermark model to roll the callbacks for checkpointing.
   * @param callback new completed callback
   * @return the list of callbacks for checkpointing
   */
  public List<TaskCallbackImpl> updateCallback(TaskCallbackImpl callback) {
    if (maxConcurrency > 1) {
      // Use the completedCallbacks queue to handle the out-of-order case when max concurrency is larger than 1
      return completedCallbacks.update(callback);
    } else {
      return ImmutableList.of(callback);
    }
  }
}
