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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class allows scheduling a Runnable actions after some debounce time.
 * When the same action is scheduled it needs to cancel the previous one. To accomplish that we keep the previous
 * future in a map, keyed by the action name. Here we predefine some actions, which are used in the
 * ZK based standalone app.
 */
public class ScheduleAfterDebounceTime {
  public static final Logger LOGGER = LoggerFactory.getLogger(ScheduleAfterDebounceTime.class);
  public static final long TIMEOUT_MS = 1000 * 10; // timeout to wait for a task to complete

  // Here we predefine some actions which are used in the ZK based standalone app.
  // Action name when the JobModel version changes
  public static final String JOB_MODEL_VERSION_CHANGE = "JobModelVersionChange";

  // Action name when the Processor membership changes
  public static final String ON_PROCESSOR_CHANGE = "OnProcessorChange";

  public static final int DEBOUNCE_TIME_MS = 2000;

  private final Callback callback;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("debounce-thread-%d").setDaemon(true).build());
  private final Map<String, ScheduledFuture> futureHandles = new HashMap<>();

  public ScheduleAfterDebounceTime() {
    this.callback = null;
  }

  public ScheduleAfterDebounceTime(Callback errorCallback) {
    this.callback = errorCallback;
  }

  synchronized public void scheduleAfterDebounceTime(String actionName, long debounceTimeMs, Runnable runnable) {
    // check if this action has been scheduled already
    ScheduledFuture sf = futureHandles.get(actionName);
    if (sf != null && !sf.isDone()) {
      LOGGER.info("cancel future for " + actionName);
      // attempt to cancel
      if (!sf.cancel(false)) {
        try {
          sf.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          // we ignore the exception
          LOGGER.warn("cancel for action " + actionName + " failed with ", e);
        }
      }
      futureHandles.remove(actionName);
    }
    // schedule a new task
    sf = scheduledExecutorService.schedule(() -> {
        try {
          runnable.run();
          LOGGER.debug(actionName + " completed successfully.");
        } catch (Exception e) {
          LOGGER.error(actionName + " threw an exception.", e);
          if (callback != null) {
            callback.onException(e);
          }
        }
      },
     debounceTimeMs,
     TimeUnit.MILLISECONDS);
    LOGGER.info("scheduled " + actionName + " in " + debounceTimeMs);
    futureHandles.put(actionName, sf);
  }

  public void stopScheduler() {
    // shutdown executor service
    scheduledExecutorService.shutdown();
  }

  interface Callback {
    void onException(Exception e);
  }
}
