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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows scheduling a Runnable actions after some debounce time.
 * When the same action is scheduled it needs to cancel the previous one. To accomplish that we keep the previous
 * future in a map, keyed by the action name.
 */
public class ScheduleAfterDebounceTime {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleAfterDebounceTime.class);
  private static final String DEBOUNCE_THREAD_NAME_FORMAT = "Samza Debounce Thread-%s";

  // timeout to wait for a task to complete.
  private static final int TIMEOUT_MS = 1000 * 10;

  /**
   * {@link ScheduledTaskCallback} associated with the scheduler. OnError method of the
   * callback will be invoked on first scheduled task failure.
   */
  private Optional<ScheduledTaskCallback> scheduledTaskCallback;

  // Responsible for scheduling delayed actions.
  private final ScheduledExecutorService scheduledExecutorService;

  /**
   * A map from actionName to {@link ScheduledFuture} of task scheduled for execution.
   */
  private final Map<String, ScheduledFuture> futureHandles = new ConcurrentHashMap<>();
  private volatile boolean isShuttingDown;

  public ScheduleAfterDebounceTime(String processorId) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(String.format(DEBOUNCE_THREAD_NAME_FORMAT, processorId))
        .setDaemon(true).build();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    isShuttingDown = false;
  }

  public void setScheduledTaskCallback(ScheduledTaskCallback scheduledTaskCallback) {
    this.scheduledTaskCallback = Optional.ofNullable(scheduledTaskCallback);
  }

  /**
   * Performs the following operations in sequential order.
   * <ul>
   *    <li> Makes best effort to cancel any existing task in task queue associated with the action.</li>
   *    <li> Schedules the incoming action for later execution and records its future.</li>
   * </ul>
   *
   * @param actionName the name of scheduleable action.
   * @param delayInMillis the time from now to delay execution.
   * @param runnable the action to execute.
   */
  public synchronized void scheduleAfterDebounceTime(String actionName, long delayInMillis, Runnable runnable) {
    if (!isShuttingDown) {
      // 1. Try to cancel any existing scheduled task associated with the action.
      tryCancelScheduledAction(actionName);

      // 2. Schedule the action.
      ScheduledFuture scheduledFuture =
          scheduledExecutorService.schedule(getScheduleableAction(actionName, runnable), delayInMillis, TimeUnit.MILLISECONDS);

      LOG.info("Scheduled action: {} to run after: {} milliseconds.", actionName, delayInMillis);
      futureHandles.put(actionName, scheduledFuture);
    } else {
      LOG.info("Scheduler is stopped. Not scheduling action: {} to run.", actionName);
    }
  }


  public synchronized void cancelAction(String action) {
    if (!isShuttingDown) {
      this.tryCancelScheduledAction(action);
    }
  }


  /**
   * Stops the scheduler. After this invocation no further schedule calls will be accepted
   * and all pending enqueued tasks will be cancelled.
   */
  public synchronized void stopScheduler() {
    if (isShuttingDown) {
      LOG.debug("Debounce timer shutdown is already in progress!");
      return;
    }

    isShuttingDown = true;
    LOG.info("Shutting down debounce timer!");

    // changing it back to use shutdown instead to prevent interruptions on the active task
    scheduledExecutorService.shutdown();

    // should clear out the future handles as well
    futureHandles.keySet()
        .forEach(this::tryCancelScheduledAction);
  }

  /**
   * Tries to cancel the task that belongs to {@code actionName} submitted to the queue.
   *
   * @param actionName the name of action to cancel.
   */
  private void tryCancelScheduledAction(String actionName) {
    LOG.info("Trying to cancel the action: {}.", actionName);
    ScheduledFuture scheduledFuture = futureHandles.get(actionName);
    if (scheduledFuture != null && !scheduledFuture.isDone()) {
      LOG.info("Attempting to cancel the future of action: {}", actionName);
      // Attempt to cancel
      if (!scheduledFuture.cancel(false)) {
        try {
          scheduledFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          // we ignore the exception
          LOG.warn("Cancelling the future of action: {} failed.", actionName, e);
        }
      }
      futureHandles.remove(actionName);
    }
  }

  /**
   * Decorate the executable action with exception handlers to facilitate cleanup on failures.
   *
   * @param actionName the name of the scheduleable action.
   * @param runnable the action to execute.
   * @return the executable action decorated with exception handlers.
   */
  private Runnable getScheduleableAction(String actionName, Runnable runnable) {
    return () -> {
      try {
        if (!isShuttingDown) {
          runnable.run();
          /*
           * Expects all run() implementations <b>not to swallow the interrupts.</b>
           * This thread is interrupted from an external source(mostly executor service) to die.
           */
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("Action: {} is interrupted.", actionName);
            doCleanUpOnTaskException(new InterruptedException());
          } else {
            LOG.info("Action: {} completed successfully.", actionName);
          }
        }
      } catch (Throwable throwable) {
        LOG.error("Execution of action: {} failed.", actionName, throwable);
        doCleanUpOnTaskException(throwable);
      }
    };
  }

  /**
   * Handler method to invoke on a exception during an scheduled task execution and which
   * the following operations in sequential order.
   * <ul>
   *   <li> Stop the scheduler. If the task execution fails or a task is interrupted, scheduler will not accept/execute any new tasks.</li>
   *   <li> Invokes the onError handler method if taskCallback is defined.</li>
   * </ul>
   *
   * @param throwable the exception happened during task execution.
   */
  private void doCleanUpOnTaskException(Throwable throwable) {
    stopScheduler();

    scheduledTaskCallback.ifPresent(callback -> callback.onError(throwable));
  }

  /**
   * A ScheduledTaskCallback::onError() is invoked on first occurrence of exception
   * when executing a task. Provides plausible hook for handling failures
   * in an asynchronous scheduled task execution.
   */
  interface ScheduledTaskCallback {
    void onError(Throwable throwable);
  }
}
