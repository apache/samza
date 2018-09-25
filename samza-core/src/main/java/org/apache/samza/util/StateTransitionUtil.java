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


public class StateTransitionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StateTransitionUtil.class);

  /**
   *
   * @param reference
   * @param expectedValues
   * @param value
   * @param <T>
   * @return
   */
  public static <T> boolean compareAndSet(AtomicReference<T> reference, Set<T> expectedValues, T value) {
    T currentValue = reference.get();
    if (expectedValues.contains(currentValue)) {
      return reference.compareAndSet(currentValue, value);
    }

    return false;
  }

  /**
   *
   * @param reference
   * @param blacklistedValues
   * @param value
   * @param <T>
   * @return
   */
  public static <T> boolean compareNotInAndSet(AtomicReference<T> reference, Set<T> blacklistedValues, T value) {
    T currentValue = reference.get();
    if (blacklistedValues.contains(currentValue)) {
      return false;
    }

    return reference.compareAndSet(currentValue, value);
  }

  /**
   *
   * @param reference
   * @param currentState
   * @param desiredState
   * @param barrier
   * @param timeout
   * @param <T>
   * @return
   */
  public static <T> boolean transitionWithBarrier(AtomicReference<T> reference, T currentState, T desiredState,
      CountDownLatch barrier, Duration timeout) {
    if (reference.get() != currentState) {
      LOG.error("Failed to transition from {} to {}", currentState, desiredState);
      throw new IllegalStateException("Cannot transition to " + desiredState + " from state "  + currentState
          + " since the current state is " + reference.get());
    }

    try {
      if (timeout.isNegative()) {
        barrier.await();
      } else {
        boolean completed = barrier.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          LOG.error("Failed to transition from {} to {} due to barrier timeout.", currentState, desiredState);
          return false;
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to transition from {} to {} due to {}", new Object[] {currentState, desiredState, e});
      return false;
    }

    return reference.compareAndSet(currentState, desiredState);
  }
}
