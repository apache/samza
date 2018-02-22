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

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class TestEmbeddedTaggedRateLimiter {

  private int testInterval = 200; // ms
  private int numberOfTasks = 2;
  private int targetRateRed = 1000;
  private int targetRatePerTaskRed = targetRateRed / numberOfTasks;
  private int targetRateGreen = 2000;
  private int targetRatePerTaskGreen = targetRateGreen / numberOfTasks;
  private int increment = 2;

  @Test
  public void testAcquire() {
    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", increment);
    tagToCredits.put("green", increment);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      rateLimiter.acquire(tagToCredits);
      tagToCount.put("red", tagToCount.get("red") + increment);
      tagToCount.put("green", tagToCount.get("green") + increment);
    }

    {
      long rate = tagToCount.get("red") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskRed);
    } {
      // Note: due to the blocking, green is capped at red's QPS
      long rate = tagToCount.get("green") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskRed);
    }
  }

  @Test
  public void testTryAcquire() {

    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", increment);
    tagToCredits.put("green", increment);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      Map<String, Integer> resultMap = rateLimiter.tryAcquire(tagToCredits);
      tagToCount.put("red", tagToCount.get("red") + resultMap.get("red"));
      tagToCount.put("green", tagToCount.get("green") + resultMap.get("green"));
    }

    {
      long rate = tagToCount.get("red") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskRed);
    } {
      long rate = tagToCount.get("green") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskGreen);
    }
  }

  @Test
  public void testAcquireWithTimeout() {

    RateLimiter rateLimiter = createRateLimiter();

    Map<String, Integer> tagToCount = new HashMap<>();
    tagToCount.put("red", 0);
    tagToCount.put("green", 0);

    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", increment);
    tagToCredits.put("green", increment);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      Map<String, Integer> resultMap = rateLimiter.acquire(tagToCredits, 20, MILLISECONDS);
      tagToCount.put("red", tagToCount.get("red") + resultMap.get("red"));
      tagToCount.put("green", tagToCount.get("green") + resultMap.get("green"));
    }

    {
      long rate = tagToCount.get("red") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskRed);
    } {
      // Note: due to the blocking, green is capped at red's QPS
      long rate = tagToCount.get("green") * 1000 / testInterval;
      verifyRate(rate, targetRatePerTaskRed);
    }
  }

  @Test(expected = IllegalArgumentException.class)
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
    TestEmbeddedRateLimiter.initRateLimiter(rateLimiter, 1);
    rateLimiter.acquire(1);
  }

  private void verifyRate(long rate, long targetRate) {
    junit.framework.Assert.assertTrue(Math.abs(rate - targetRate) <= 10 * increment * 1000 / testInterval);
  }


  private RateLimiter createRateLimiter() {
    Map<String, Integer> tagToTargetRateMap = new HashMap<>();
    tagToTargetRateMap.put("red", targetRateRed);
    tagToTargetRateMap.put("green", targetRateGreen);
    RateLimiter rateLimiter = new EmbeddedTaggedRateLimiter(tagToTargetRateMap);
    TestEmbeddedRateLimiter.initRateLimiter(rateLimiter, numberOfTasks);
    return rateLimiter;
  }

}
