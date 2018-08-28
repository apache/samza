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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.samza.SamzaException;

import net.jodah.failsafe.RetryPolicy;


/**
 * Common retry policy parameters for table IO. This serves as an abstraction on top of
 * retry libraries. This common policy supports below features:
 *  - backoff modes: fixed, random, exponential
 *  - termination modes: by attempts, by duration
 *  - jitter
 *
 * Retry libraries can implement a subset or all features as described by this common policy.
 * Currently, the policy object can be translated into {@link RetryPolicy} of failsafe library.
 */
public class TableRetryPolicy implements Serializable {
  enum BackoffType { NONE, FIXED, RANDOM, EXPONENTIAL }

  // Backoff parameters
  private long sleepMs;
  private long randomMinMs;
  private long randomMaxMs;
  private double exponentialFactor;
  private long exponentialMaxSleepMs;
  private long jitterMs;

  // By default no early termination
  private int maxAttempts = -1;
  private long maxDelayMs = Long.MAX_VALUE;

  // By default no backoff during retries
  private BackoffType backoffType = BackoffType.NONE;

  /**
   * Set the sleep time for the fixed backoff policy.
   * @param sleepMs sleep time in milliseconds
   * @return this policy instance
   */
  public TableRetryPolicy withFixedBackoff(long sleepMs) {
    this.sleepMs = sleepMs;
    this.backoffType = BackoffType.FIXED;
    return this;
  }

  /**
   * Set the sleep time for the random backoff policy. The actual sleep time
   * before each attempt is randomly selected between {@code [minSleepMs, maxSleepMs]}
   * @param minSleepMs lower bound sleep time in milliseconds
   * @param maxSleepMs upper bound sleep time in milliseconds
   * @return this policy instance
   */
  public TableRetryPolicy withRandomBackoff(long minSleepMs, long maxSleepMs) {
    this.randomMinMs = minSleepMs;
    this.randomMaxMs = maxSleepMs;
    this.backoffType = BackoffType.RANDOM;
    return this;
  }

  /**
   * Set the parameters for the exponential backoff policy. The actual sleep time
   * is exponentially incremented up to the {@code maxSleepMs} and multiplying
   * successive delays by the {@code factor}.
   * @param sleepMs initial sleep time in milliseconds
   * @param maxSleepMs upper bound sleep time in milliseconds
   * @param factor exponential factor for backoff
   * @return this policy instance
   */
  public TableRetryPolicy withExponentialBackoff(long sleepMs, long maxSleepMs, double factor) {
    this.sleepMs = sleepMs;
    this.exponentialMaxSleepMs = maxSleepMs;
    this.exponentialFactor = factor;
    this.backoffType = BackoffType.EXPONENTIAL;
    return this;
  }

  /**
   * Set the jitter for the backoff policy to provide additional randomness.
   * If this is set, a random value between {@code [0, jitter]} will be added
   * to each sleep time.
   * @param jitterMs initial sleep time in milliseconds
   * @return this policy instance
   */
  public TableRetryPolicy withJitter(long jitterMs) {
    this.jitterMs = jitterMs;
    return this;
  }

  /**
   * Set maximum number of attempts before terminating the operation.
   * @param maxAttempts number of attempts
   * @return this policy instance
   */
  public TableRetryPolicy withStopAfterAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  /**
   * Set maximum total delay (sleep + execution) before terminating the operation.
   * @param maxDelayMs delay time in milliseconds
   * @return this policy instance
   */
  public TableRetryPolicy withStopAfterDelay(long maxDelayMs) {
    this.maxDelayMs = maxDelayMs;
    return this;
  }

  /**
   * Convert the TableRetryPolicy to failsafe {@link RetryPolicy}.
   * @param isRetriable predicate to signal retriable exceptions.
   * @return this policy instance
   */
  RetryPolicy toFailsafePolicy(Predicate<Throwable> isRetriable) {
    RetryPolicy policy = new RetryPolicy();
    policy.withMaxDuration(maxDelayMs, TimeUnit.MILLISECONDS);
    policy.withMaxRetries(maxAttempts);
    if (jitterMs != 0) {
      policy.withJitter(jitterMs, TimeUnit.MILLISECONDS);
    }
    policy.retryOn((e) -> isRetriable.test(e));

    switch (backoffType) {
      case NONE:
        break;

      case FIXED:
        policy.withDelay(sleepMs, TimeUnit.MILLISECONDS);
        break;

      case RANDOM:
        policy.withDelay(randomMinMs, randomMaxMs, TimeUnit.MILLISECONDS);
        break;

      case EXPONENTIAL:
        policy.withBackoff(sleepMs, exponentialMaxSleepMs, TimeUnit.MILLISECONDS, exponentialFactor);
        break;

      default:
        throw new SamzaException("Unknown retry policy type.");
    }

    return policy;
  }
}
