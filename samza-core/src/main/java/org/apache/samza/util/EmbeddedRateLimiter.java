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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;


/**
 * An embedded rate limiter
 */
public class EmbeddedRateLimiter implements RateLimiter {

  private int targetRate;
  private com.google.common.util.concurrent.RateLimiter rateLimiter;

  public EmbeddedRateLimiter(int creditsPerSecond) {
    this.targetRate = creditsPerSecond;
  }

  @Override
  public void acquire(int numberOfCredits) {
    ensureInitialized();
    rateLimiter.acquire(numberOfCredits);
  }

  @Override
  public int acquire(int numberOfCredit, long timeout, TimeUnit unit) {
    ensureInitialized();
    if (rateLimiter.tryAcquire(numberOfCredit, timeout, unit)) {
      return numberOfCredit;
    } else {
      return 0;
    }
  }

  @Override
  public int tryAcquire(int numberOfCredit) {
    ensureInitialized();
    if (rateLimiter.tryAcquire(numberOfCredit)) {
      return numberOfCredit;
    } else {
      return 0;
    }
  }

  @Override
  public void acquire(Map<String, Integer> tagToCreditsMap) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public Map<String, Integer> acquire(Map<String, Integer> tagToCreditsMap, long timeout, TimeUnit unit) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public Map<String, Integer> tryAcquire(Map<String, Integer> tagToCreditsMap) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public void init(Config config, TaskContext taskContext) {
    int numberOfTasks = taskContext.getSamzaContainerContext().taskNames.size();
    this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(
        targetRate / numberOfTasks);
  }

  private void ensureInitialized() {
    Preconditions.checkArgument(rateLimiter != null, "Not initialized");
  }

}
