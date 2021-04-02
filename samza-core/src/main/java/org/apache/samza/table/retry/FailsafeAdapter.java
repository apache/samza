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

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.RetryPolicy;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.samza.SamzaException;


/**
 * Helper class adapting the generic {@link TableRetryPolicy} to a failsafe {@link RetryPolicy} and
 * creating failsafe retryer instances with proper metrics management.
 */
class FailsafeAdapter {
  /**
   * Convert the {@link TableRetryPolicy} to failsafe {@link RetryPolicy}.
   * @return this policy instance
   */
  static RetryPolicy valueOf(TableRetryPolicy policy) {
    // max retries default changed to 2 in v2.0. switching back to infinite retries by default for back compat.
    RetryPolicy failSafePolicy = new RetryPolicy().withMaxRetries(-1);

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
        failSafePolicy.withBackoff(policy.getSleepTime().toMillis(), policy.getExponentialMaxSleep().toMillis(),
            ChronoUnit.MILLIS, policy.getExponentialFactor());
        break;

      default:
        throw new SamzaException("Unknown retry policy type.");
    }

    if (policy.getMaxDuration() != null) {
      failSafePolicy.withMaxDuration(policy.getMaxDuration());
    }
    if (policy.getMaxAttempts() != null) {
      failSafePolicy.withMaxRetries(policy.getMaxAttempts());
    }
    if (policy.getJitter() != null && policy.getBackoffType() != TableRetryPolicy.BackoffType.RANDOM) {
      failSafePolicy.withJitter(policy.getJitter());
    }

    failSafePolicy.abortOn(policy.getRetryPredicate().negate());
    return failSafePolicy;
  }

  /**
   * Obtain an async failsafe retryer instance with the specified policy, metrics, and executor service.
   * @param retryPolicy retry policy
   * @param metrics retry metrics
   * @param retryExec executor service for scheduling async retries
   * @return {@link net.jodah.failsafe.FailsafeExecutor} instance
   */
  static <T> FailsafeExecutor<T> failsafe(RetryPolicy<T> retryPolicy, RetryMetrics metrics, ScheduledExecutorService retryExec) {
    long startMs = System.currentTimeMillis();

    RetryPolicy<T> retryPolicyWithMetrics = retryPolicy
        .onRetry(e -> metrics.retryCount.inc())
        .onRetriesExceeded(e -> {
          metrics.retryTimer.update(System.currentTimeMillis() - startMs);
          metrics.permFailureCount.inc();
        }).onSuccess((e) -> {
          if (e.getAttemptCount() > 1) {
            metrics.retryTimer.update(System.currentTimeMillis() - startMs);
          } else {
            metrics.successCount.inc();
          }
        });

    return Failsafe.with(retryPolicyWithMetrics).with(retryExec);
  }
}
