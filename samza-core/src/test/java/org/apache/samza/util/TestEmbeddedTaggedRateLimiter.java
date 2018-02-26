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

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class TestEmbeddedTaggedRateLimiter {

  final static private int TEST_INTERVAL = 200; // ms
  final static private int NUMBER_OF_TASKS = 2;
  final static private int TARGET_RATE_RED = 1000;
  final static private int TARGET_RATE_PER_TASK_RED = TARGET_RATE_RED / NUMBER_OF_TASKS;
  final static private int TARGET_RATE_GREEN = 2000;
  final static private int TARGET_RATE_PER_TASK_GREEN = TARGET_RATE_GREEN / NUMBER_OF_TASKS;
  final static private int INCREMENT = 2;

  @Test
  public void testAcquire() {
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
  public void testTryAcquire() {

    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", INCREMENT);
    tagToCredits.put("green", INCREMENT);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TEST_INTERVAL) {
      Map<String, Integer> resultMap = rateLimiter.tryAcquire(tagToCredits);
      tagToCount.put("red", tagToCount.get("red") + resultMap.get("red"));
      tagToCount.put("green", tagToCount.get("green") + resultMap.get("green"));
    }

    {
      long rate = tagToCount.get("red") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_RED);
    } {
      long rate = tagToCount.get("green") * 1000 / TEST_INTERVAL;
      verifyRate(rate, TARGET_RATE_PER_TASK_GREEN);
    }
  }

  @Test
  public void testAcquireWithTimeout() {

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
  public void testFailsWhenUninitialized() {
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
    TestEmbeddedRateLimiter.initRateLimiter(rateLimiter);
    rateLimiter.acquire(1);
  }

  private void verifyRate(long rate, long targetRate) {
    // As the actual rate would likely not be exactly the same as target rate, the calculation below
    // verifies the actual rate is within 5% of the target rate per task
    Assert.assertTrue(Math.abs(rate - targetRate) <= targetRate * 5 / 100);
  }

  private RateLimiter createRateLimiter() {
    Map<String, Integer> tagToTargetRateMap = new HashMap<>();
    tagToTargetRateMap.put("red", TARGET_RATE_RED);
    tagToTargetRateMap.put("green", TARGET_RATE_GREEN);
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(tagToTargetRateMap);
    TestEmbeddedRateLimiter.initRateLimiter(rateLimiter);
    return rateLimiter;
  }

}
