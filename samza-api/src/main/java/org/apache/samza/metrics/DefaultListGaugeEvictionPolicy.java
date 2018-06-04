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
package org.apache.samza.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Provides an eviction policy that evicts entries from the valueList if
 * a.) There are more elements in the valueList than the specified maxNumberOfItems (removal in FIFO order), or
 * b.) There are elements which have timestamps which are stale as compared to currentTime (the staleness bound is
 * specified as maxStaleness).
 *
 * This naive implementation uses a periodic thread with a configurable period.
 */
public class DefaultListGaugeEvictionPolicy<T> implements ListGaugeEvictionPolicy<T> {

  private final Queue<ListGauge.ValueInfo<T>> valueList;
  private final int nItems;
  private final Duration durationThreshold;
  private final ScheduledExecutorService scheduledExecutorService;

  public DefaultListGaugeEvictionPolicy(Queue<ListGauge.ValueInfo<T>> valueList, int maxNumberOfItems,
      Duration maxStaleness, Duration period) {
    this.valueList = valueList;
    this.nItems = maxNumberOfItems;
    this.durationThreshold = maxStaleness;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.scheduledExecutorService.schedule(new EvictionRunnable(), period.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void elementAddedCallback() {

    // need to synchronize here because this thread could be concurrent with the runnable thread and can
    // cause two vals to be removed (wrong eviction) even if a threadsafe queue was used.
    synchronized (this.valueList) {
      int numToEvict = this.valueList.size() - nItems;

      while (numToEvict > 0) {
        this.valueList.poll(); // remove head
        numToEvict--;
      }
    }
  }

  private class EvictionRunnable implements Runnable {

    @Override
    public void run() {
      Instant currentTimestamp = Instant.now();

      synchronized (valueList) {
        ListGauge.ValueInfo<T> valueInfo = valueList.peek();

        // continue remove-head if currenttimestamp - head-element's timestamp > durationThreshold
        while (valueInfo != null
            && Duration.between(valueInfo.insertTimestamp, currentTimestamp).compareTo(durationThreshold) > 0) {
          valueList.poll();
          valueInfo = valueList.peek();
        }
      }
    }
  }
}
