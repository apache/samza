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
  public static final String DEFAULT_BLOB_STORE_MANAGER_FACTORY = "com.linkedin.samza.ambry.AmbryBlobStoreManagerFactory";
  public static final String BLOB_STORE_ADMIN_FACTORY = PREFIX + "admin.factory";
  public static final String DEFAULT_BLOB_STORE_ADMIN_FACTORY = "com.linkedin.samza.ambry.AmbryBlobStoreAdminFactory";
  // Configs related to retry policy of blob stores
  public static final String BLOB_STORE_RETRY_POLICY_MAX_RETRIES = PREFIX + "retry.policy.max.retries";
  // -1 for RetryPolicy means unlimited retries. Retry is limited by max retry duration, rather than count of retries.
  public static final int DEFAULT_BLOB_STORE_RETRY_POLICY_MAX_RETRIES = -1;
  public static final String BLOB_STORE_RETRY_POLICY_MAX_RETRIES_DURATION_MILLIS = PREFIX + "retry.policy.max.retires.duration.millis";
  public static final long DEFAULT_BLOB_STORE_RETRY_POLICY_MAX_RETRIES_DURATION_MILLIS = 10*60*1000; // 10 mins
  public static final String BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_MILLIS = PREFIX + "retry.policy.backoff.delay.millis";
  public static final long DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_MILLIS = 100;
  public static final String BLOB_STORE_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS = PREFIX + "retry.policy.backoff.max.delay.millis";
  public static final long DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS = 312500;
  public static final String BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_FACTOR = PREFIX + "retry.policy.backoff.delay.factor";
  public static final int DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_FACTOR = 5;

  public BlobStoreConfig(Config config) {
    super(config);
  }

  public String getBlobStoreManagerFactory() {
    return get(BLOB_STORE_MANAGER_FACTORY);
  }

  public String getBlobStoreAdminFactory() {
    return get(BLOB_STORE_ADMIN_FACTORY);
  }

  public int getBlobStoreRetryPolicyMaxRetries() {
    return getInt(BLOB_STORE_RETRY_POLICY_MAX_RETRIES, DEFAULT_BLOB_STORE_RETRY_POLICY_MAX_RETRIES);
  }

  public long getBlobStoreRetryPolicyMaxRetriesDurationMillis() {
    return getLong(BLOB_STORE_RETRY_POLICY_MAX_RETRIES_DURATION_MILLIS,
        DEFAULT_BLOB_STORE_RETRY_POLICY_MAX_RETRIES_DURATION_MILLIS);
  }

  public long getBlobStoreRetryPolicyBackoffDelayMillis() {
    return getLong(BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_MILLIS, DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_MILLIS);
  }

  public long getBlobStoreRetryPolicyBackoffMaxDelayMillis() {
    return getLong(BLOB_STORE_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS,
        DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_MAX_DELAY_MILLIS);
  }

  public int getBlobStoreRetryPolicyBackoffDelayFactor() {
    return getInt(BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_FACTOR, DEFAULT_BLOB_STORE_RETRY_POLICY_BACKOFF_DELAY_FACTOR);
  }
}
