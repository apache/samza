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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.task.TaskContext;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestEmbeddedTaggedRateLimiter {

  final static private int TEST_INTERVAL = 200; // ms
  final static private int NUMBER_OF_TASKS = 2;
  final static private int TARGET_RATE_RED = 1000;
  final static private int TARGET_RATE_PER_TASK_RED = TARGET_RATE_RED / NUMBER_OF_TASKS;
  final static private int TARGET_RATE_GREEN = 2000;
  final static private int INCREMENT = 2;

  final static private int TARGET_RATE = 4000;
  final static private int TARGET_RATE_PER_TASK = TARGET_RATE / NUMBER_OF_TASKS;

  @Test
  public void testAcquire() {
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(TARGET_RATE);
    initRateLimiter(rateLimiter);

    int count = 0;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TEST_INTERVAL) {
      rateLimiter.acquire(INCREMENT);
      count += INCREMENT;
    }

    long rate = count * 1000 / TEST_INTERVAL;
    verifyRate(rate);
  }

  @Test
  public void testAcquireWithTimeout() {
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(TARGET_RATE);
    initRateLimiter(rateLimiter);

    boolean hasSeenZeros = false;

    int count = 0;
    int callCount = 0;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TEST_INTERVAL) {
      ++callCount;
      int availableCredits = rateLimiter.acquire(INCREMENT, 20, MILLISECONDS);
      if (availableCredits <= 0) {
        hasSeenZeros = true;
      } else {
        count += INCREMENT;
      }
    }

    long rate = count * 1000 / TEST_INTERVAL;
    verifyRate(rate);
    junit.framework.Assert.assertTrue(Math.abs(callCount - TARGET_RATE_PER_TASK * TEST_INTERVAL / 1000 / INCREMENT) <= 2);
    junit.framework.Assert.assertFalse(hasSeenZeros);
  }

  @Test(expected = IllegalStateException.class)
  public void testFailsWhenUninitialized() {
    new EmbeddedTaggedRateLimiter(100).acquire(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsWhenUsingTags() {
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(10);
    initRateLimiter(rateLimiter);
    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", 1);
    tagToCredits.put("green", 1);
    rateLimiter.acquire(tagToCredits);
  }

  @Test
  public void testAcquireTagged() {
    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", INCREMENT);
    tagToCredits.put("green", INCREMENT);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TEST_INTERVAL) {
      rateLimiter.acquire(tagToCredits);
      tagToCount.put("red", tagToCount.get("red") + INCREMENT);
      tagToCount.put("green", tagToCount.get("green") + INCREMENT);
    }

    {
      long rate = tagToCount.get("red") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_RED);
    } {
      // Note: due to blocking, green is capped at red's QPS
      long rate = tagToCount.get("green") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_RED);
    }
  }

  @Test
  public void testAcquireWithTimeoutTagged() {

    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", INCREMENT);
    tagToCredits.put("green", INCREMENT);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TEST_INTERVAL) {
      Map<String, Integer> resultMap = rateLimiter.acquire(tagToCredits, 20, MILLISECONDS);
      tagToCount.put("red", tagToCount.get("red") + resultMap.get("red"));
      tagToCount.put("green", tagToCount.get("green") + resultMap.get("green"));
    }

    {
      long rate = tagToCount.get("red") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_RED);
    } {
      // Note: due to blocking, green is capped at red's QPS
      long rate = tagToCount.get("green") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_RED);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testFailsWhenUninitializedTagged() {
    Map<String, Integer> tagToTargetRateMap = new HashMap<>();
    tagToTargetRateMap.put("red", 1000);
    tagToTargetRateMap.put("green", 2000);
    new EmbeddedTaggedRateLimiter(tagToTargetRateMap).acquire(tagToTargetRateMap);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsWhenNotUsingTags() {
    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", 1);
    tagToCredits.put("green", 1);
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(tagToCredits);
    initRateLimiter(rateLimiter);
    rateLimiter.acquire(1);
  }

  private void verifyRate(long rate, long targetRate) {
    // As the actual rate would likely not be exactly the same as target rate, the calculation below
    // verifies the actual rate is within 10% of the target rate per task
    Assert.assertTrue(Math.abs(rate - targetRate) <= targetRate * 10 / 100);
  }

  private RateLimiter createRateLimiter() {
    Map<String, Integer> tagToTargetRateMap = new HashMap<>();
    tagToTargetRateMap.put("red", TARGET_RATE_RED);
    tagToTargetRateMap.put("green", TARGET_RATE_GREEN);
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(tagToTargetRateMap);
    initRateLimiter(rateLimiter);
    return rateLimiter;
  }

  private void verifyRate(long rate) {
    // As the actual rate would likely not be exactly the same as target rate, the calculation below
    // verifies the actual rate is within 5% of the target rate per task
    junit.framework.Assert.assertTrue(Math.abs(rate - TARGET_RATE_PER_TASK) <= TARGET_RATE_PER_TASK * 5 / 100);
  }

  static void initRateLimiter(RateLimiter rateLimiter) {
    Config config = mock(Config.class);
    TaskContext taskContext = mock(TaskContext.class);
    SamzaContainerContext containerContext = mockSamzaContainerContext();
    when(taskContext.getSamzaContainerContext()).thenReturn(containerContext);
    rateLimiter.init(config, taskContext);
  }

  static SamzaContainerContext mockSamzaContainerContext() {
    try {
      Collection<String> taskNames = mock(Collection.class);
      when(taskNames.size()).thenReturn(NUMBER_OF_TASKS);
      SamzaContainerContext containerContext = mock(SamzaContainerContext.class);
      Field taskNamesField = SamzaContainerContext.class.getDeclaredField("taskNames");
      taskNamesField.setAccessible(true);
      taskNamesField.set(containerContext, taskNames);
      taskNamesField.setAccessible(false);
      return containerContext;
    } catch (Exception ex) {
      throw new SamzaException(ex);
    }
  }
}
