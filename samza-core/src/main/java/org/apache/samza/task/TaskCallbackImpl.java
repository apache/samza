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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements {@link TaskCallback}. It triggers the
 * {@link TaskCallbackListener} with the callback result. If the
 * callback is called multiple times, it will throw IllegalStateException
 * to the listener.
 */
class TaskCallbackImpl implements TaskCallback, Comparable<TaskCallbackImpl> {
  private static final Logger log = LoggerFactory.getLogger(TaskCallbackImpl.class);

  final TaskName taskName;
  final IncomingMessageEnvelope envelope;
  final ReadableCoordinator coordinator;
  final long timeCreatedNs;
  private final AtomicBoolean isComplete = new AtomicBoolean(false);
  private final TaskCallbackListener listener;
  private ScheduledFuture scheduledFuture = null;
  private final long seqNum;

  public TaskCallbackImpl(TaskCallbackListener listener,
      TaskName taskName,
      IncomingMessageEnvelope envelope,
      ReadableCoordinator coordinator,
      long seqNum,
      long timeCreatedNs) {
    this.listener = listener;
    this.taskName = taskName;
    this.envelope = envelope;
    this.coordinator = coordinator;
    this.seqNum = seqNum;
    this.timeCreatedNs = timeCreatedNs;
  }

  @Override
  public void complete() {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
    log.trace("Callback complete for task {}, ssp {}, offset {}.",
        new Object[] {taskName, envelope.getSystemStreamPartition(), envelope.getOffset()});

    if (isComplete.compareAndSet(false, true)) {
      listener.onComplete(this);
    } else {
      String msg = String.format("Callback complete was invoked after completion for task %s, ssp %s, offset %s.",
          taskName, envelope.getSystemStreamPartition(), envelope.getOffset());
      listener.onFailure(this, new IllegalStateException(msg));
    }
  }

  @Override
  public void failure(Throwable t) {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }

    if (isComplete.compareAndSet(false, true)) {
      String msg = String.format("Callback failed for task %s, ssp %s, offset %s.",
          taskName, envelope.getSystemStreamPartition(), envelope.getOffset());
      listener.onFailure(this, new SamzaException(msg, t));
    } else {
      String msg = String.format("Task callback failure was invoked after completion for task %s, ssp %s, offset %s.",
          taskName, envelope.getSystemStreamPartition(), envelope.getOffset());
      listener.onFailure(this, new IllegalStateException(msg, t));
    }
  }

  void setScheduledFuture(ScheduledFuture scheduledFuture) {
    this.scheduledFuture = scheduledFuture;
  }

  @Override
  public int compareTo(TaskCallbackImpl callback) {
    return Long.compare(this.seqNum, callback.seqNum);
  }

  boolean matchSeqNum(long seqNum) {
    return this.seqNum == seqNum;
  }
}
