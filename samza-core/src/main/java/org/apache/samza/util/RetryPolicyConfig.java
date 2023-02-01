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

import java.time.temporal.ChronoUnit;


public class RetryPolicyConfig {
  private final int maxRetries;
  private final long maxDuration;
  private final double jitterFactor;
  private final long backoffDelay;
  private final long backoffMaxDelay;
  private final int backoffDelayFactor;
  private final ChronoUnit unit;

  public RetryPolicyConfig(int maxRetries, long maxDuration, double jitterFactor, long backoffDelay, long backoffMaxDelay,
      int backoffDelayFactor, ChronoUnit unit) {
    this.maxRetries = maxRetries;
    this.maxDuration = maxDuration;
    this.jitterFactor = jitterFactor;
    this.backoffDelay = backoffDelay;
    this.backoffMaxDelay = backoffMaxDelay;
    this.backoffDelayFactor = backoffDelayFactor;
    this.unit = unit;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public ChronoUnit getUnit() {
    return unit;
  }

  public int getBackoffDelayFactor() {
    return backoffDelayFactor;
  }

  public long getBackoffMaxDelay() {
    return backoffMaxDelay;
  }

  public long getBackoffDelay() {
    return backoffDelay;
  }

  public double getJitterFactor() {
    return jitterFactor;
  }

  public long getMaxDuration() {
    return maxDuration;
  }
}
