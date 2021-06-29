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

package org.apache.samza.table.retry;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.samza.SamzaException;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.RetryPolicy;


/**
 * Helper class adapting the generic {@link TableRetryPolicy} to a failsafe {@link RetryPolicy} and
 * creating failsafe retryer instances with proper metrics management.
 */
class FailsafeAdapter {
  /**
   * Convert the {@link TableRetryPolicy} to failsafe {@link RetryPolicy}.
   * @return this policy instance
   */
  static RetryPolicy<?> valueOf(TableRetryPolicy policy) {
    RetryPolicy<?> failSafePolicy = new RetryPolicy<>();

    switch (policy.getBackoffType()) {
      case NONE:
        break;

      case FIXED:
        failSafePolicy.withDelay(policy.getSleepTime());
        break;

      case RANDOM:
        failSafePolicy.withDelay(policy.getRandomMin().toMillis(), policy.getRandomMax().toMillis(), ChronoUnit.MILLIS);
        break;

      case EXPONENTIAL:
        failSafePolicy.withBackoff(policy.getSleepTime().toMillis(), policy.getExponentialMaxSleep().toMillis(), ChronoUnit.MILLIS,
            policy.getExponentialFactor());
        break;

      default:
        throw new SamzaException("Unknown retry policy type.");
    }

    if (policy.getMaxDuration() != null) {
      failSafePolicy.withMaxDuration(policy.getMaxDuration());
    }
    if (policy.getMaxAttempts() != null) {
      failSafePolicy.withMaxRetries(policy.getMaxAttempts());
    } else {
      failSafePolicy.withMaxRetries(-1);
    }
    if (policy.getJitter() != null && policy.getBackoffType() != TableRetryPolicy.BackoffType.RANDOM) {
      failSafePolicy.withJitter(policy.getJitter());
    }

    failSafePolicy.abortOn(e -> !policy.getRetryPredicate().test(e));

    return failSafePolicy;
  }

  /**
   * Obtain an async failsafe retryer instance with the specified policy, metrics, and executor service.
   * @param retryPolicy retry policy
   * @param metrics retry metrics
   * @param retryExec executor service for scheduling async retries
   * @return {@link net.jodah.failsafe.FailsafeExecutor} instance
   */
  static <T> FailsafeExecutor<T> failsafe(RetryPolicy<?> retryPolicy, RetryMetrics metrics, ScheduledExecutorService retryExec) {
    long startMs = System.currentTimeMillis();
    RetryPolicy<?> updatedRetryPolicy = retryPolicy
        .onRetry(e -> metrics.retryCount.inc())
        .onRetriesExceeded(e -> {
          metrics.retryTimer.update(System.currentTimeMillis() - startMs);
          metrics.permFailureCount.inc();
        });
    return (FailsafeExecutor<T>) Failsafe.with(updatedRetryPolicy).with(retryExec)
        .onSuccess(executionCompletedEvent -> {
          if (executionCompletedEvent.getAttemptCount() > 1) {
            metrics.retryTimer.update(System.currentTimeMillis() - startMs);
          } else {
            metrics.successCount.inc();
          }
        });
  }
}
