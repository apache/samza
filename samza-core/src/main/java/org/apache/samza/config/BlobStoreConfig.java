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

package org.apache.samza.config;

/**
 * Config related helper methods for BlobStore.
 */
public class BlobStoreConfig extends MapConfig {

  private static final String PREFIX = "blob.store.";
  public static final String BLOB_STORE_MANAGER_FACTORY = PREFIX + "manager.factory";
  public static final String BLOB_STORE_ADMIN_FACTORY = PREFIX + "admin.factory";
  // Configs related to retry policy of blob stores
  private static final String RETRY_POLICY_PREFIX = PREFIX + "retry.policy.";
  public static final String RETRY_POLICY_MAX_RETRIES = RETRY_POLICY_PREFIX + "max.retries";
  // -1 for RetryPolicy means unlimited retries. Retry is limited by max retry duration, rather than count of retries.
  public static final int DEFAULT_RETRY_POLICY_MAX_RETRIES = -1;
  public static final String RETRY_POLICY_MAX_RETRY_DURATION_MILLIS = RETRY_POLICY_PREFIX + "max.retry.duration.millis";
  public static final long DEFAULT_RETRY_POLICY_MAX_RETRY_DURATION_MILLIS = 10 * 60 * 1000; // 10 mins
  public static final String RETRY_POLICY_BACKOFF_DELAY_MILLIS = RETRY_POLICY_PREFIX + "backoff.delay.millis";
  public static final long DEFAULT_RETRY_POLICY_BACKOFF_DELAY_MILLIS = 100;
  public static final String RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS = RETRY_POLICY_PREFIX + "backoff.max.delay.millis";
  public static final long DEFAULT_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS = 312500;
  public static final String RETRY_POLICY_BACKOFF_DELAY_FACTOR = RETRY_POLICY_PREFIX + "backoff.delay.factor";
  public static final int DEFAULT_RETRY_POLICY_BACKOFF_DELAY_FACTOR = 5;
  public static final String RETRY_POLICY_JITTER_FACTOR_MILLIS =  RETRY_POLICY_PREFIX + "jitter.factor.millis";
  // random retry delay between -100 to 100 millisecond
  public static final long DEFAULT_RETRY_POLICY_JITTER_FACTOR_MILLIS = 100;

  public BlobStoreConfig(Config config) {
    super(config);
  }

  public String getBlobStoreManagerFactory() {
    return get(BLOB_STORE_MANAGER_FACTORY);
  }

  public String getBlobStoreAdminFactory() {
    return get(BLOB_STORE_ADMIN_FACTORY);
  }

  public int getRetryPolicyMaxRetries() {
    return getInt(RETRY_POLICY_MAX_RETRIES, DEFAULT_RETRY_POLICY_MAX_RETRIES);
  }

  public long getRetryPolicyMaxRetriesDurationMillis() {
    return getLong(RETRY_POLICY_MAX_RETRY_DURATION_MILLIS, DEFAULT_RETRY_POLICY_MAX_RETRY_DURATION_MILLIS);
  }

  public long getRetryPolicyBackoffDelayMillis() {
    return getLong(RETRY_POLICY_BACKOFF_DELAY_MILLIS, DEFAULT_RETRY_POLICY_BACKOFF_DELAY_MILLIS);
  }

  public long getRetryPolicyBackoffMaxDelayMillis() {
    return getLong(RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS, DEFAULT_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS);
  }

  public int getRetryPolicyBackoffDelayFactor() {
    return getInt(RETRY_POLICY_BACKOFF_DELAY_FACTOR, DEFAULT_RETRY_POLICY_BACKOFF_DELAY_FACTOR);
  }

  public long getRetryPolicyJitterFactorMillis() {
    return getLong(RETRY_POLICY_JITTER_FACTOR_MILLIS, DEFAULT_RETRY_POLICY_JITTER_FACTOR_MILLIS);
  }
}
