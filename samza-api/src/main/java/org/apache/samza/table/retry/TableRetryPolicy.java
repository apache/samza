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
import java.time.Duration;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;


/**
 * Common retry policy parameters for table IO. This serves as an abstraction on top of
 * retry libraries. This common policy supports below features:
 *  - backoff modes: fixed, random, exponential
 *  - termination modes: by attempts, by duration
 *  - jitter
 *
 * Retry libraries can implement a subset or all features as described by this common policy.
 */
public class TableRetryPolicy implements Serializable {
  enum BackoffType {
    /**
     * No backoff in between two retry attempts.
     */
    NONE,

    /**
     * Backoff by a fixed duration {@code sleepTime}.
     */
    FIXED,

    /**
     * Backoff by a randomly selected duration between {@code minSleep} and {@code maxSleep}.
     */
    RANDOM,

    /**
     * Backoff by exponentially increasing durations by {@code exponentialFactor} starting from {@code sleepTime}.
     */
    EXPONENTIAL
  }

  // Backoff parameters
  private Duration sleepTime;
  private Duration randomMin;
  private Duration randomMax;
  private double exponentialFactor;
  private Duration exponentialMaxSleep;
  private Duration jitter;

  // By default no early termination
  private Integer maxAttempts = null;
  private Duration maxDuration = null;

  // By default no backoff during retries
  private BackoffType backoffType = BackoffType.NONE;

  /**
   * Serializable adapter interface for {@link java.util.function.Predicate}.
   * This is needed because TableRetryPolicy needs to be serializable as part of the
   * table config whereas {@link java.util.function.Predicate} is not serializable.
   */
  public interface RetryPredicate extends Predicate<Throwable>, Serializable {
  }

  // By default no custom retry predicate so retry decision is made solely by the table functions
  private RetryPredicate retryPredicate = (ex) -> false;

  /**
   * Set the sleepTime time for the fixed backoff policy.
   * @param sleepTime sleepTime time
   * @return this policy instance
   */
  public TableRetryPolicy withFixedBackoff(Duration sleepTime) {
    Preconditions.checkNotNull(sleepTime);
    this.sleepTime = sleepTime;
    this.backoffType = BackoffType.FIXED;
    return this;
  }

  /**
   * Set the sleepTime time for the random backoff policy. The actual sleepTime time
   * before each attempt is randomly selected between {@code [minSleep, maxSleep]}
   * @param minSleep lower bound sleepTime time
   * @param maxSleep upper bound sleepTime time
   * @return this policy instance
   */
  public TableRetryPolicy withRandomBackoff(Duration minSleep, Duration maxSleep) {
    Preconditions.checkNotNull(minSleep);
    Preconditions.checkNotNull(maxSleep);
    this.randomMin = minSleep;
    this.randomMax = maxSleep;
    this.backoffType = BackoffType.RANDOM;
    return this;
  }

  /**
   * Set the parameters for the exponential backoff policy. The actual sleepTime time
   * is exponentially incremented up to the {@code maxSleep} and multiplying
   * successive delays by the {@code factor}.
   * @param sleepTime initial sleepTime time
   * @param maxSleep upper bound sleepTime time
   * @param factor exponential factor for backoff
   * @return this policy instance
   */
  public TableRetryPolicy withExponentialBackoff(Duration sleepTime, Duration maxSleep, double factor) {
    Preconditions.checkNotNull(sleepTime);
    Preconditions.checkNotNull(maxSleep);
    this.sleepTime = sleepTime;
    this.exponentialMaxSleep = maxSleep;
    this.exponentialFactor = factor;
    this.backoffType = BackoffType.EXPONENTIAL;
    return this;
  }

  /**
   * Set the jitter for the backoff policy to provide additional randomness.
   * If this is set, a random value between {@code [0, jitter]} will be added
   * to each sleepTime time. This applies to {@code FIXED} and {@code EXPONENTIAL}
   * modes only.
   * @param jitter initial sleepTime time
   * @return this policy instance
   */
  public TableRetryPolicy withJitter(Duration jitter) {
    Preconditions.checkNotNull(jitter);
    if (backoffType != BackoffType.RANDOM) {
      this.jitter = jitter;
    }
    return this;
  }

  /**
   * Set maximum number of attempts before terminating the operation.
   * @param maxAttempts number of attempts
   * @return this policy instance
   */
  public TableRetryPolicy withStopAfterAttempts(int maxAttempts) {
    Preconditions.checkArgument(maxAttempts >= 0);
    this.maxAttempts = maxAttempts;
    return this;
  }

  /**
   * Set maximum total delay (sleepTime + execution) before terminating the operation.
   * @param maxDelay delay time
   * @return this policy instance
   */
  public TableRetryPolicy withStopAfterDelay(Duration maxDelay) {
    Preconditions.checkNotNull(maxDelay);
    this.maxDuration = maxDelay;
    return this;
  }

  /**
   * Set the predicate to use for identifying retriable exceptions. If specified, table
   * retry logic will consult both such predicate and table function and retry will be
   * attempted if either option returns true.
   * @param retryPredicate predicate for retriable exception identification
   * @return this policy instance
   */
  public TableRetryPolicy withRetryPredicate(RetryPredicate retryPredicate) {
    Preconditions.checkNotNull(retryPredicate);
    this.retryPredicate = retryPredicate;
    return this;
  }

  /**
   * @return initial/fixed sleep time.
   */
  public Duration getSleepTime() {
    return sleepTime;
  }

  /**
   * @return lower sleepTime time for random backoff or null if {@code policyType} is not {@code RANDOM}.
   */
  public Duration getRandomMin() {
    return randomMin;
  }

  /**
   * @return upper sleepTime time for random backoff or null if {@code policyType} is not {@code RANDOM}.
   */
  public Duration getRandomMax() {
    return randomMax;
  }

  /**
   * @return exponential factor for exponential backoff.
   */
  public double getExponentialFactor() {
    return exponentialFactor;
  }

  /**
   * @return maximum sleepTime time for exponential backoff or null if {@code policyType} is not {@code EXPONENTIAL}.
   */
  public Duration getExponentialMaxSleep() {
    return exponentialMaxSleep;
  }

  /**
   * Introduce randomness to the sleepTime time.
   * @return jitter to add on to each backoff or null if not set.
   */
  public Duration getJitter() {
    return jitter;
  }

  /**
   * Termination after a fix number of attempts.
   * @return maximum number of attempts without success before giving up the operation or null if not set.
   */
  public Integer getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * Termination after a fixed duration.
   * @return maximum duration without success before giving up the operation or null if not set.
   */
  public Duration getMaxDuration() {
    return maxDuration;
  }

  /**
   * @return type of the backoff.
   */
  public BackoffType getBackoffType() {
    return backoffType;
  }

  /**
   * @return Custom predicate for retriable exception identification or null if not specified.
   */
  public RetryPredicate getRetryPredicate() {
    return retryPredicate;
  }
}
