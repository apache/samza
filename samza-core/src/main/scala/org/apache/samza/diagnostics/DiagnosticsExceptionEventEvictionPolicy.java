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
package org.apache.samza.diagnostics;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.samza.metrics.ListGauge;
import org.apache.samza.metrics.RetainLastNPolicy;


/**
 * Provides an eviction policy that evicts entries from the ListGauge if
 * a.) There are more elements in the listGauge than the specified maxNumberOfItems (removal in FIFO order), or
 * b.) There are elements which have timestamps which are stale as compared to currentTime (the staleness bound is
 * specified as maxStaleness).
 *
 * This naive implementation uses a periodic thread with a configurable period
 * TODO: an event-schedule based implementation that schedules an "eviction" inside onElementAdded callback.
 */
public class DiagnosticsExceptionEventEvictionPolicy extends RetainLastNPolicy<DiagnosticsExceptionEvent> {

  private final Duration durationThreshold;
  private final ScheduledExecutorService scheduledExecutorService;

  /**
   * Construct a {@link DiagnosticsExceptionEventEvictionPolicy}
   * @param listGauge The list gauge that is to be used with this policy
   * @param maxNumberOfItems The max number of items that can remain in the listgauge
   * @param maxStaleness The max staleness of items permitted in the listgauge
   * @param period The periodicity with which the listGauge would be checked for stale values
   */
  public DiagnosticsExceptionEventEvictionPolicy(ListGauge<DiagnosticsExceptionEvent> listGauge, int maxNumberOfItems,
      Duration maxStaleness, Duration period) {
    super(listGauge, maxNumberOfItems);
    this.durationThreshold = maxStaleness;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.scheduledExecutorService.schedule(new EvictionRunnable(), period.toMillis(), TimeUnit.MILLISECONDS);
  }

  private class EvictionRunnable implements Runnable {
    @Override
    public void run() {
      long currentTimestamp = System.currentTimeMillis();

      // compute the list of events tso remove based on their timestamps, current time, and durationThreshold
      Stream<DiagnosticsExceptionEvent> itemsToRemove =
          listGauge.getValue().stream().filter(x -> currentTimestamp - x.getTimestamp() > durationThreshold.toMillis());

      itemsToRemove.forEach(x -> listGauge.remove(x));
    }
  }
}
