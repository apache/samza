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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * An embedded rate limiter that supports tags
 */
public class EmbeddedTaggedRateLimiter implements RateLimiter {

  static final private Logger LOGGER = LoggerFactory.getLogger(EmbeddedTaggedRateLimiter.class);

  private final Map<String, Integer> tagToTargetRateMap;
  private Map<String, com.google.common.util.concurrent.RateLimiter> tagToRateLimiterMap;

  public EmbeddedTaggedRateLimiter(Map<String, Integer> tagToCreditsPerSecondMap) {
    Preconditions.checkArgument(tagToCreditsPerSecondMap.size() > 0, "Map of tags can't be empty");
    this.tagToTargetRateMap = tagToCreditsPerSecondMap;
  }

  @Override
  public void acquire(Map<String, Integer> tagToCreditsMap) {
    ensureTagsAreValid(tagToCreditsMap);
    tagToCreditsMap.forEach((tag, numberOfCredits) -> tagToRateLimiterMap.get(tag).acquire(numberOfCredits));
  }

  @Override
  public Map<String, Integer> acquire(Map<String, Integer> tagToCreditsMap, long timeout, TimeUnit unit) {
    ensureTagsAreValid(tagToCreditsMap);

    long timeoutInNanos = NANOSECONDS.convert(timeout, unit);

    Stopwatch stopwatch = Stopwatch.createStarted();
    return tagToCreditsMap.entrySet().stream()
        .map(e -> {
            String tag = e.getKey();
            int requiredCredits = e.getValue();
            long remainingTimeoutInNanos = Math.max(0L, timeoutInNanos - stopwatch.elapsed(NANOSECONDS));
            com.google.common.util.concurrent.RateLimiter rateLimiter = tagToRateLimiterMap.get(tag);
            int availableCredits = rateLimiter.tryAcquire(requiredCredits, remainingTimeoutInNanos, NANOSECONDS)
                ? requiredCredits
                : 0;
            return new ImmutablePair<String, Integer>(tag, availableCredits);
          })
        .collect(Collectors.toMap(ImmutablePair::getKey, ImmutablePair::getValue));
  }

  @Override
  public Map<String, Integer> tryAcquire(Map<String, Integer> tagToCreditsMap) {
    ensureTagsAreValid(tagToCreditsMap);
    return tagToCreditsMap.entrySet().stream()
        .map(e -> {
            String tag = e.getKey();
            int requiredCredits = e.getValue();
            int availableCredits = tagToRateLimiterMap.get(tag).tryAcquire(requiredCredits)
                ? requiredCredits
                : 0;
            return new ImmutablePair<String, Integer>(tag, availableCredits);
          })
        .collect(Collectors.toMap(ImmutablePair::getKey, ImmutablePair::getValue));
  }

  @Override
  public void acquire(int numberOfCredits) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public int acquire(int numberOfCredit, long timeout, TimeUnit unit) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public int tryAcquire(int numberOfCredit) {
    throw new IllegalArgumentException("This method is not applicable");
  }

  @Override
  public void init(Config config, TaskContext taskContext) {
    this.tagToRateLimiterMap = Collections.unmodifiableMap(tagToTargetRateMap.entrySet().stream()
        .map(e -> {
            String tag = e.getKey();
            int effectiveRate = e.getValue();
            if (taskContext != null) {
              effectiveRate /= taskContext.getSamzaContainerContext().taskNames.size();
              LOGGER.info(String.format("Effective rate limit for task %s and tag %s is %d",
                  taskContext.getTaskName(), tag, effectiveRate));
            }
            return new ImmutablePair<String, com.google.common.util.concurrent.RateLimiter>(
                tag, com.google.common.util.concurrent.RateLimiter.create(effectiveRate));
          })
        .collect(Collectors.toMap(ImmutablePair::getKey, ImmutablePair::getValue))
    );
  }

  private void ensureInitialized() {
    Preconditions.checkState(tagToRateLimiterMap != null, "Not initialized");
  }

  private void ensureTagsAreValid(Map<String, ?> tagMap) {
    ensureInitialized();
    tagMap.keySet().forEach(tag ->
        Preconditions.checkArgument(tagToRateLimiterMap.containsKey(tag), "Invalid tag: " + tag));
  }

}
