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
package org.apache.samza.util;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class to perform complex state transitions.
 */
public class StateTransitionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StateTransitionUtil.class);

  /**
   * Atomically sets the value to the given updated value if current value is one of the expected values.
   *
   * @param reference the atomic reference
   * @param expectedValues set of expected values
   * @param update the new value
   * @param <T> type of the atomic reference
   *
   * @return true if current state is one of the expected value and transition was successful; false otherwise
   */
  public static <T> boolean compareAndSet(AtomicReference<T> reference, Set<T> expectedValues, T update) {
    while (true) {
      T currentValue = reference.get();
      if (expectedValues.contains(currentValue)) {
        if (reference.compareAndSet(currentValue, update)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Atomically sets the value to the given updated value if the black listed values does not contain current value.
   *
   * @param reference the atomic reference
   * @param blacklistedValues set of blacklisted values
   * @param update the new value
   * @param <T> type of the atomic reference
   *
   * @return true if current state is not in the blacklisted values and transition was successful; false otherwise
   */
  public static <T> boolean compareNotInAndSet(AtomicReference<T> reference, Set<T> blacklistedValues, T update) {
    while (true) {
      T currentValue = reference.get();
      if (blacklistedValues.contains(currentValue)) {
        return false;
      }

      if (reference.compareAndSet(currentValue, update)) {
        return true;
      }
    }
  }

  /**
   * Atomically sets the value to the updated value if the current value == expected value and the barrier
   * latch counts down to zero. If the barrier times out determined by the timeout parameter, the atomic reference
   * is not updated.
   *
   * @param reference the atomic reference
   * @param expect the expected value
   * @param update the new value
   * @param barrier the barrier latch
   * @param timeout the timeout for barrier
   * @param <T> type of the atomic reference
   *
   * @return true if current state == expected value and the barrier completed and transition was successful; false otherwise
   */
  public static <T> boolean transitionWithBarrier(AtomicReference<T> reference, T expect, T update,
      CountDownLatch barrier, Duration timeout) {
    if (reference.get() != expect) {
      LOG.error("Failed to transition from {} to {}", expect, update);
      throw new IllegalStateException("Cannot transition to " + update + " from state "  + expect
          + " since the current state is " + reference.get());
    }

    try {
      if (timeout.isNegative()) {
        barrier.await();
      } else {
        boolean completed = barrier.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          LOG.error("Failed to transition from {} to {} due to barrier timeout.", expect, update);
          return false;
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to transition from {} to {} due to {}", new Object[] {expect, update, e});
      return false;
    }

    return reference.compareAndSet(expect, update);
  }
}
