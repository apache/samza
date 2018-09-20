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
package org.apache.samza.container.host;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link SystemStatisticsMonitor} for unix and mac platforms. Users can implement their own
 * ways of getting {@link SystemMemoryStatistics} and provide a {@link SystemStatisticsGetter} implementation. The default
 * behavior is to rely on unix commands like ps to obtain {@link SystemMemoryStatistics}
 *
 * All callback invocations are from the same thread - hence, are guaranteed to be serialized. An exception thrown
 * from a callback will suppress all subsequent callbacks. If the execution of a
 * {@link org.apache.samza.container.host.SystemStatisticsMonitor.Listener} callback takes longer than the polling
 * interval, subsequent callback invocations may start late but will not be invoked concurrently.
 *
 * This class is thread-safe.
 */
public class StatisticsMonitorImpl implements SystemStatisticsMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsMonitorImpl.class);

  /**
   * Polling interval of this monitor. The monitor will report host statistics periodically via a callback
   * after pollingIntervalMillis, pollingIntervalMillis *2, pollingIntervalMillis * 3 and so on.
   *
   */
  private final long pollingIntervalMillis;


  // Use a private lock instead of synchronized because an instance of StatisticsMonitorImpl could be used as a
  // lock else-where.
  private final Object lock = new Object();

  // Single threaded executor to handle callback invocations.
  private final ScheduledExecutorService schedulerService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("Samza StatisticsMonitor Thread-%d").setDaemon(true).build());

  // Use this as a set with value always set to True
  private final ConcurrentMap<StatisticsMonitorImpl.Listener, Boolean> listenerSet = new ConcurrentHashMap<>();
  private final SystemStatisticsGetter statisticsGetter;

  /**
   * Tracks the state of the monitor. Typical state transitions from INIT (when the monitor is created) to RUNNING (when
   * the start is invoked on the monitor) to STOPPED (when stop is invoked)
   */
  private enum State { INIT, RUNNING, STOPPED }
  private volatile State currentState;


  /**
   * Creates a new {@link StatisticsMonitorImpl} that reports statistics every 60 seconds
   *
   */
  public StatisticsMonitorImpl() {
    this(60000, new PosixCommandBasedStatisticsGetter());
  }

  /**
   * Creates a new {@link StatisticsMonitorImpl} that reports statistics periodically

   * @param pollingIntervalMillis The polling interval to report statistics.
   * @param statisticsGetter the getter to gather system stats info
   */
  public StatisticsMonitorImpl(long pollingIntervalMillis, SystemStatisticsGetter statisticsGetter) {
    this.pollingIntervalMillis = pollingIntervalMillis;
    this.statisticsGetter = statisticsGetter;
    currentState = State.INIT;
  }

  @Override
  public void start() {
    synchronized (lock) {
      switch (currentState) {
        case INIT:
          currentState = State.RUNNING;
          schedulerService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              sampleStatistics();
            }
          }, pollingIntervalMillis, pollingIntervalMillis, TimeUnit.MILLISECONDS);
          break;

        case RUNNING:
          return;

        case STOPPED:
          throw new IllegalStateException("Attempting to start an already stopped StatisticsMonitor");
      }
    }
  }

  private void sampleStatistics() {
    SystemMemoryStatistics statistics = null;
    try {
      statistics = statisticsGetter.getSystemMemoryStatistics();
    } catch (Throwable e) {
      LOG.error("Error during obtaining statistics: ", e);
    }

    for (Listener listener : listenerSet.keySet()) {
      if (statistics != null) {
        try {
          // catch all exceptions to shield one listener from exceptions thrown by others.
          listener.onUpdate(statistics);
        } catch (Throwable e) {
          // delete this listener so that it does not receive future callbacks.
          listenerSet.remove(listener);
          LOG.error("Listener threw an exception: ", e);
        }
      }
    }
  }


  /**
   * Stops the monitor. Once the monitor is stopped, no new samples will be delivered to the listeners. If stop is
   * invoked during the period a {@link org.apache.samza.container.host.SystemStatisticsMonitor.Listener} callback is
   * under execution, may cause the callback to be interrupted.
   */

  @Override
  public void stop() {
    synchronized (lock) {
      schedulerService.shutdownNow();
      listenerSet.clear();
      currentState = State.STOPPED;
    }
  }

  /**
   * @see org.apache.samza.container.host.SystemStatisticsMonitor.Listener#registerListener(Listener)
   */
  @Override
  public boolean registerListener(Listener listener) {
    synchronized (lock) {
      if (currentState == State.STOPPED) {
        LOG.error("Attempting to register a listener after monitor was stopped.");
        return false;
      } else {
        if (listenerSet.containsKey(listener)) {
          LOG.error("Attempting to register an already registered listener");
          return false;
        }
        listenerSet.put(listener, Boolean.TRUE);
        return true;
      }
    }
  }
}
