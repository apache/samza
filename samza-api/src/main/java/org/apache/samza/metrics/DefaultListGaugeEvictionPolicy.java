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
import org.apache.samza.util.TimestampedValue;


/**
 * Provides an eviction policy that evicts entries from the elements if
 * a.) There are more elements in the elements than the specified maxNumberOfItems (removal in FIFO order), or
 * b.) There are elements which have timestamps which are stale as compared to currentTime (the staleness bound is
 * specified as maxStaleness).
 *
 */
public class DefaultListGaugeEvictionPolicy<T> {

  /**
   * Evicts entries from the elements list, based on the given item-size and durationThreshold.
   * Callers are responsible for thread-safety.
   */
  public void evict(Queue<TimestampedValue<T>> elements, int maxNumberOfItems, Duration maxStaleness) {
    this.evictBasedOnSize(elements, maxNumberOfItems);
    this.evictBasedOnTimestamp(elements, maxStaleness);
  }

  /**
   * Evicts entries from elements in FIFO order until it has maxNumberOfItems
   * @param elements queue to evict elements from
   * @param maxNumberOfItems max number of items to be left in the queue
   */
  private void evictBasedOnSize(Queue<TimestampedValue<T>> elements, int maxNumberOfItems) {
    int numToEvict = elements.size() - maxNumberOfItems;
    while (numToEvict > 0) {
      elements.poll(); // remove head
      numToEvict--;
    }
  }

  /**
   * Removes entries from elements to ensure no element has a timestamp more than maxStaleness before current timestamp.
   * @param elements the queue to evict elements from
   * @param maxStaleness max staleness permitted in elements
   */
  private void evictBasedOnTimestamp(Queue<TimestampedValue<T>> elements, Duration maxStaleness) {
    Instant currentTimestamp = Instant.now();
    TimestampedValue<T> valueInfo = elements.peek();

    // continue remove-head if currenttimestamp - head-element's timestamp > durationThreshold
    while (valueInfo != null && currentTimestamp.toEpochMilli() - valueInfo.getTimestamp() > maxStaleness.toMillis()) {
      elements.poll();
      valueInfo = elements.peek();
    }
  }
}
