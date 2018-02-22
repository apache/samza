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
import org.junit.Test;

import junit.framework.Assert;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestEmbeddedRateLimiter {

  private int testInterval = 200; // ms
  private int targetRate = 4000;
  private int numberOfTasks = 2;
  private int targetRatePerTask = targetRate / numberOfTasks;
  private int increment = 2;

  @Test
  public void testAcquire() {
    RateLimiter rateLimiter = new EmbeddedRateLimiter(targetRate);
    initRateLimiter(rateLimiter, numberOfTasks);

    int count = 0;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      rateLimiter.acquire(increment);
      count += increment;
    }

    long rate = count * 1000 / testInterval;
    verifyRate(rate);
  }

  @Test
  public void testTryAcquire() {
    RateLimiter rateLimiter = new EmbeddedRateLimiter(targetRate);
    initRateLimiter(rateLimiter, numberOfTasks);

    boolean hasSeenZeros = false;

    int count = 0;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      int availableCredits = rateLimiter.tryAcquire(increment);
      if (availableCredits <= 0) {
        hasSeenZeros = true;
      } else {
        count += increment;
      }
    }

    long rate = count * 1000 / testInterval;
    verifyRate(rate);
    Assert.assertTrue(hasSeenZeros);
  }

  @Test
  public void testAcquireWithTimeout() {
    RateLimiter rateLimiter = new EmbeddedRateLimiter(targetRate);
    initRateLimiter(rateLimiter, numberOfTasks);

    boolean hasSeenZeros = false;

    int count = 0;
    int callCount = 0;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < testInterval) {
      ++callCount;
      int availableCredits = rateLimiter.acquire(increment, 20, MILLISECONDS);
      if (availableCredits <= 0) {
        hasSeenZeros = true;
      } else {
        count += increment;
      }
    }

    long rate = count * 1000 / testInterval;
    verifyRate(rate);
    Assert.assertTrue(Math.abs(callCount - targetRatePerTask * testInterval / 1000 / increment) <= 2);
    Assert.assertFalse(hasSeenZeros);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsWhenUninitialized() {
    new EmbeddedRateLimiter(100).acquire(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsWhenUsingTags() {
    RateLimiter rateLimiter = new EmbeddedRateLimiter(10);
    initRateLimiter(rateLimiter, 1);
    Map<String, Integer> tagToCredits = new HashMap<>();
    tagToCredits.put("red", 1);
    tagToCredits.put("green", 1);
    rateLimiter.acquire(tagToCredits);
  }

  private void verifyRate(long rate) {
    Assert.assertTrue(Math.abs(rate - targetRatePerTask) <= 10 * increment * 1000 / testInterval);
  }

  static void initRateLimiter(RateLimiter rateLimiter, int numberOfTasks) {
    Config config = mock(Config.class);
    TaskContext taskContext = mock(TaskContext.class);
    SamzaContainerContext containerContext = mockSamzaContainerContext(numberOfTasks);
    when(taskContext.getSamzaContainerContext()).thenReturn(containerContext);
    rateLimiter.init(config, taskContext);
  }

  static SamzaContainerContext mockSamzaContainerContext(int numberOfTasks) {
    try {
      Collection<String> taskNames = mock(Collection.class);
      when(taskNames.size()).thenReturn(numberOfTasks);
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
