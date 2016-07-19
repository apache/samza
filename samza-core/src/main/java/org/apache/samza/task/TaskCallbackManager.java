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

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;


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
     * @return callback of highest watermark needed to be committed
     */
    TaskCallbackImpl update(TaskCallbackImpl cb) {
      synchronized (lock) {
        callbacks.add(cb);

        TaskCallbackImpl callback = null;
        TaskCallbackImpl callbackToCommit = null;
        TaskCoordinator.RequestScope shutdownRequest = null;
        // look for the last contiguous callback
        while (!callbacks.isEmpty() && callbacks.peek().matchSeqNum(nextSeqNum)) {
          ++nextSeqNum;
          callback = callbacks.poll();

          if (callback.coordinator.commitRequest().isDefined()) {
            callbackToCommit = callback;
          }

          if (callback.coordinator.shutdownRequest().isDefined()) {
            shutdownRequest = callback.coordinator.shutdownRequest().get();
          }
        }

        // if there is no manual commit, use the highest contiguous callback message offset
        if (callbackToCommit == null) {
          callbackToCommit = callback;
        }

        // if there is a shutdown request, merge it into the coordinator to commit
        if (shutdownRequest != null) {
          callbackToCommit.coordinator.shutdown(shutdownRequest);
        }

        return callbackToCommit;
      }
    }
  }

  private long seqNum = 0L;
  private final AtomicInteger pendingCount = new AtomicInteger(0);
  private final TaskCallbacks completeCallbacks = new TaskCallbacks();
  private final TaskInstanceMetrics metrics;
  private final ScheduledExecutorService timer;
  private final TaskCallbackListener listener;
  private long timeout;

  public TaskCallbackManager(TaskCallbackListener listener, TaskInstanceMetrics metrics, ScheduledExecutorService timer, long timeout) {
    this.listener = listener;
    this.metrics = metrics;
    this.timer = timer;
    this.timeout = timeout;
  }

  public TaskCallbackImpl createCallback(TaskName taskName,
      IncomingMessageEnvelope envelope,
      ReadableCoordinator coordinator) {
    final TaskCallbackImpl callback = new TaskCallbackImpl(listener, taskName, envelope, coordinator, seqNum++);
    int count = pendingCount.incrementAndGet();
    metrics.messagesInFlight().set(count);

    if (timer != null) {
      Runnable timerTask = new Runnable() {
        @Override
        public void run() {
          String msg = "Task " + callback.taskName + " callback times out";
          callback.failure(new TaskCallbackTimeoutException(msg));
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
   * @param success callback result status
   * @return the callback for checkpointing
   */
  public TaskCallbackImpl updateCallback(TaskCallbackImpl callback, boolean success) {
    TaskCallbackImpl callbackToCommit = null;
    if (success) {
      callbackToCommit = completeCallbacks.update(callback);
    }
    int count = pendingCount.decrementAndGet();
    metrics.messagesInFlight().set(count);
    return callbackToCommit;
  }
}
