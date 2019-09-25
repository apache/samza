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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContextImpl;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * An embedded rate limiter that supports tags. A default tag will be used if users specifies a simple rate only
 * for simple use cases.
 */
public class EmbeddedTaggedRateLimiter implements RateLimiter {
  static final private Logger LOGGER = LoggerFactory.getLogger(EmbeddedTaggedRateLimiter.class);
  private static final String DEFAULT_TAG = "default-tag";
  private static final Map<String, Integer> DEFAULT_TAG_MAP = Collections.singletonMap(DEFAULT_TAG, 0);

  private final Map<String, Integer> tagToTargetRateMap;
  private Map<String, com.google.common.util.concurrent.RateLimiter> tagToRateLimiterMap;
  private boolean initialized;

  public EmbeddedTaggedRateLimiter(int creditsPerSecond) {
    this(Collections.singletonMap(DEFAULT_TAG, creditsPerSecond));
  }

  public EmbeddedTaggedRateLimiter(Map<String, Integer> tagToCreditsPerSecondMap) {
    Preconditions.checkArgument(tagToCreditsPerSecondMap.size() > 0, "Map of tags can't be empty");
    tagToCreditsPerSecondMap.values().forEach(c -> Preconditions.checkArgument(c >= 0, "Credits must be non-negative"));
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
            return new ImmutablePair<>(tag, availableCredits);
          })
        .collect(Collectors.toMap(ImmutablePair::getKey, ImmutablePair::getValue));
  }

  @Override
  public Set<String> getSupportedTags() {
    return Collections.unmodifiableSet(tagToRateLimiterMap.keySet());
  }

  @Override
  public void acquire(int numberOfCredits) {
    ensureTagsAreValid(DEFAULT_TAG_MAP);
    tagToRateLimiterMap.get(DEFAULT_TAG).acquire(numberOfCredits);
  }

  @Override
  public int acquire(int numberOfCredit, long timeout, TimeUnit unit) {
    ensureTagsAreValid(DEFAULT_TAG_MAP);
    return tagToRateLimiterMap.get(DEFAULT_TAG).tryAcquire(numberOfCredit, timeout, unit)
        ? numberOfCredit
        : 0;
  }

  @Override
  public void init(Context context) {
    this.tagToRateLimiterMap = Collections.unmodifiableMap(tagToTargetRateMap.entrySet().stream()
        .map(e -> {
            String tag = e.getKey();
            JobModel jobModel = ((TaskContextImpl) context.getTaskContext()).getJobModel();
            int numTasks = jobModel.getContainers().values().stream()
                .mapToInt(cm -> cm.getTasks().size())
                .sum();
            double effectiveRate = (double) e.getValue() / numTasks;
            TaskName taskName = context.getTaskContext().getTaskModel().getTaskName();
            LOGGER.info(String.format("Effective rate limit for task %s and tag %s is %f", taskName, tag,
                effectiveRate));
            if (effectiveRate < 1.0) {
              LOGGER.warn(String.format("Effective limit rate (%f) is very low. "
                              + "Total rate limit is %d while number of tasks is %d. Consider increasing the rate limit.",
                        effectiveRate, e.getValue(), numTasks));
            }
            return new ImmutablePair<>(tag, com.google.common.util.concurrent.RateLimiter.create(effectiveRate));
          })
        .collect(Collectors.toMap(ImmutablePair::getKey, ImmutablePair::getValue))
    );
    initialized = true;
  }

  private void ensureInitialized() {
    Preconditions.checkState(initialized, "Not initialized");
  }

  private void ensureTagsAreValid(Map<String, ?> tagMap) {
    ensureInitialized();
    tagMap.keySet().forEach(tag ->
        Preconditions.checkArgument(tagToRateLimiterMap.containsKey(tag), "Invalid tag: " + tag));
  }

}
