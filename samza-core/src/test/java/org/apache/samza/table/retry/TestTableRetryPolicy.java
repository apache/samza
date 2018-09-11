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

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;

import net.jodah.failsafe.RetryPolicy;


public class TestTableRetryPolicy {
  @Test
  public void testNoRetry() {
    TableRetryPolicy retryPolicy = new TableRetryPolicy();
    Assert.assertEquals(TableRetryPolicy.BackoffType.NONE, retryPolicy.getBackoffType());
  }

  @Test
  public void testFixedRetry() {
    TableRetryPolicy retryPolicy = new TableRetryPolicy();
    retryPolicy.withFixedBackoff(Duration.ofMillis(1000));
    retryPolicy.withJitter(Duration.ofMillis(100));
    retryPolicy.withStopAfterAttempts(4);
    Assert.assertEquals(TableRetryPolicy.BackoffType.FIXED, retryPolicy.getBackoffType());
    RetryPolicy fsRetry = FailsafeAdapter.valueOf(retryPolicy);
    Assert.assertEquals(1000, fsRetry.getDelay().toMillis());
    Assert.assertEquals(100, fsRetry.getJitter().toMillis());
    Assert.assertEquals(4, fsRetry.getMaxRetries());
    Assert.assertNotNull(retryPolicy.getRetryOn());
  }

  @Test
  public void testRandomRetry() {
    TableRetryPolicy retryPolicy = new TableRetryPolicy();
    retryPolicy.withRandomBackoff(Duration.ofMillis(1000), Duration.ofMillis(2000));
    retryPolicy.withJitter(Duration.ofMillis(100)); // no-op
    Assert.assertEquals(TableRetryPolicy.BackoffType.RANDOM, retryPolicy.getBackoffType());
    RetryPolicy fsRetry = FailsafeAdapter.valueOf(retryPolicy);
    Assert.assertEquals(1000, fsRetry.getDelayMin().toMillis());
    Assert.assertEquals(2000, fsRetry.getDelayMax().toMillis());
  }

  @Test
  public void testExponentialRetry() {
    TableRetryPolicy retryPolicy = new TableRetryPolicy();
    retryPolicy.withExponentialBackoff(Duration.ofMillis(1000), Duration.ofMillis(2000), 1.5);
    retryPolicy.withJitter(Duration.ofMillis(100));
    Assert.assertEquals(TableRetryPolicy.BackoffType.EXPONENTIAL, retryPolicy.getBackoffType());
    RetryPolicy fsRetry = FailsafeAdapter.valueOf(retryPolicy);
    Assert.assertEquals(1000, fsRetry.getDelay().toMillis());
    Assert.assertEquals(2000, fsRetry.getMaxDelay().toMillis());
    Assert.assertEquals(1.5, fsRetry.getDelayFactor(), 0.001);
    Assert.assertEquals(100, fsRetry.getJitter().toMillis());
  }

  @Test
  public void testCustomRetryPredicate() {
    TableRetryPolicy retryPolicy = new TableRetryPolicy();
    retryPolicy.withRetryOn((e) -> e instanceof IllegalArgumentException);
    Assert.assertTrue(retryPolicy.getRetryOn().test(new IllegalArgumentException()));
    Assert.assertFalse(retryPolicy.getRetryOn().test(new NullPointerException()));
  }
}
