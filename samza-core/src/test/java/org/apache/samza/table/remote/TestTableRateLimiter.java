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

package org.apache.samza.table.remote;

import java.util.Arrays;
import java.util.Collections;

import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.util.RateLimiter;
import org.junit.Test;

import junit.framework.Assert;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestTableRateLimiter {
  private static final String DEFAULT_TAG = "mytag";

  public TableRateLimiter<String, String> getThrottler() {
    return getThrottler(DEFAULT_TAG);
  }

  public TableRateLimiter<String, String> getThrottler(String tag) {
    TableRateLimiter.CreditFunction<String, String> credFn =
        (TableRateLimiter.CreditFunction<String, String>) (key, value) -> {
      int credits = key == null ? 0 : 3;
      credits += value == null ? 0 : 3;
      return credits;
    };
    RateLimiter rateLimiter = mock(RateLimiter.class);
    doReturn(Collections.singleton(DEFAULT_TAG)).when(rateLimiter).getSupportedTags();
    TableRateLimiter<String, String> rateLimitHelper = new TableRateLimiter<>("foo", rateLimiter, credFn, tag);
    Timer timer = mock(Timer.class);
    rateLimitHelper.setTimerMetric(timer);
    return rateLimitHelper;
  }

  @Test
  public void testCreditKeyOnly() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler();
    Assert.assertEquals(3, rateLimitHelper.getCredits("abc", null));
  }

  @Test
  public void testCreditKeyValue() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler();
    Assert.assertEquals(6, rateLimitHelper.getCredits("abc", "efg"));
  }

  @Test
  public void testCreditKeys() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler();
    Assert.assertEquals(9, rateLimitHelper.getCredits(Arrays.asList("abc", "efg", "hij")));
  }

  @Test
  public void testCreditEntries() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler();
    Assert.assertEquals(12, rateLimitHelper.getEntryCredits(
        Arrays.asList(new Entry<>("abc", "efg"), new Entry<>("hij", "lmn"))));
  }

  @Test
  public void testThrottle() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler();
    Timer timer = mock(Timer.class);
    rateLimitHelper.setTimerMetric(timer);
    rateLimitHelper.throttle("foo");
    verify(rateLimitHelper.rateLimiter, times(1)).acquire(anyMap());
    verify(timer, times(1)).update(anyLong());
  }

  @Test
  public void testThrottleUnknownTag() {
    TableRateLimiter<String, String> rateLimitHelper = getThrottler("unknown_tag");
    rateLimitHelper.throttle("foo");
    verify(rateLimitHelper.rateLimiter, times(0)).acquire(anyMap());
  }
}
