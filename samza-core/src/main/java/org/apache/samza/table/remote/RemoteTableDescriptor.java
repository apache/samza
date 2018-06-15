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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.util.EmbeddedTaggedRateLimiter;
import org.apache.samza.util.RateLimiter;

import com.google.common.base.Preconditions;


/**
 * Table descriptor for remote store backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RemoteTableDescriptor<K, V> extends BaseTableDescriptor<K, V, RemoteTableDescriptor<K, V>> {
  /**
   * Tag to be used for provision credits for rate limiting read operations from the remote table.
   * Caller must pre-populate the credits with this tag when specifying a custom rate limiter instance
   * through {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, CreditFunction, CreditFunction)}
   */
  public static final String RL_READ_TAG = "readTag";

  /**
   * Tag to be used for provision credits for rate limiting write operations into the remote table.
   * Caller can optionally populate the credits with this tag when specifying a custom rate limiter instance
   * through {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, CreditFunction, CreditFunction)}
   * and it needs the write functionality.
   */
  public static final String RL_WRITE_TAG = "writeTag";

  // Input support for a specific remote store (required)
  private TableReadFunction<K, V> readFn;

  // Output support for a specific remote store (optional)
  private TableWriteFunction<K, V> writeFn;

  // Rate limiter for client-side throttling;
  // can either be constructed indirectly from rates or overridden by withRateLimiter()
  private RateLimiter rateLimiter;

  // Rates for constructing the default rate limiter when they are non-zero
  private Map<String, Integer> tagCreditsMap = new HashMap<>();

  private CreditFunction<K, V> readCreditFn;
  private CreditFunction<K, V> writeCreditFn;

  /**
   * Construct a table descriptor instance
   * @param tableId Id of the table
   */
  public RemoteTableDescriptor(String tableId) {
    super(tableId);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    // Serialize and store reader/writer functions
    tableSpecConfig.put(RemoteTableProvider.READ_FN, SerdeUtils.serialize("read function", readFn));

    if (writeFn != null) {
      tableSpecConfig.put(RemoteTableProvider.WRITE_FN, SerdeUtils.serialize("write function", writeFn));
    }

    // Serialize the rate limiter if specified
    if (!tagCreditsMap.isEmpty()) {
      rateLimiter = new EmbeddedTaggedRateLimiter(tagCreditsMap);
    }

    if (rateLimiter != null) {
      tableSpecConfig.put(RemoteTableProvider.RATE_LIMITER, SerdeUtils.serialize("rate limiter", rateLimiter));
    }

    // Serialize the readCredit and writeCredit functions
    if (readCreditFn != null) {
      tableSpecConfig.put(RemoteTableProvider.READ_CREDIT_FN, SerdeUtils.serialize(
          "read credit function", readCreditFn));
    }

    if (writeCreditFn != null) {
      tableSpecConfig.put(RemoteTableProvider.WRITE_CREDIT_FN, SerdeUtils.serialize(
          "write credit function", writeCreditFn));
    }

    return new TableSpec(tableId, serde, RemoteTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Use specified TableReadFunction with remote table.
   * @param readFn read function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withReadFunction(TableReadFunction<K, V> readFn) {
    Preconditions.checkNotNull(readFn, "null read function");
    this.readFn = readFn;
    return this;
  }

  /**
   * Use specified TableWriteFunction with remote table.
   * @param writeFn write function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteFunction(TableWriteFunction<K, V> writeFn) {
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    return this;
  }

  /**
   * Specify a rate limiter along with credit functions to map a table record (as KV) to the amount
   * of credits to be charged from the rate limiter for table read and write operations.
   * This is an advanced API that provides greater flexibility to throttle each record in the table
   * with different number of credits. For most common use-cases eg: limit the number of read/write
   * operations, please instead use the {@link RemoteTableDescriptor#withReadRateLimit(int)} and
   * {@link RemoteTableDescriptor#withWriteRateLimit(int)}.
   *
   * @param rateLimiter rate limiter instance to be used for throttling
   * @param readCreditFn credit function for rate limiting read operations
   * @param writeCreditFn credit function for rate limiting write operations
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withRateLimiter(RateLimiter rateLimiter, CreditFunction<K, V> readCreditFn,
      CreditFunction<K, V> writeCreditFn) {
    Preconditions.checkNotNull(rateLimiter, "null read rate limiter");
    this.rateLimiter = rateLimiter;
    this.readCreditFn = readCreditFn;
    this.writeCreditFn = writeCreditFn;
    return this;
  }

  /**
   * Specify the rate limit for table read operations. If the read rate limit is set with this method
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, CreditFunction, CreditFunction)}
   * and vice versa.
   * @param creditsPerSec rate limit for read operations; must be positive
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withReadRateLimit(int creditsPerSec) {
    Preconditions.checkArgument(creditsPerSec > 0, "Max read rate must be a positive number.");
    tagCreditsMap.put(RL_READ_TAG, creditsPerSec);
    return this;
  }

  /**
   * Specify the rate limit for table write operations. If the write rate limit is set with this method
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, CreditFunction, CreditFunction)}
   * and vice versa.
   * @param creditsPerSec rate limit for write operations; must be positive
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteRateLimit(int creditsPerSec) {
    Preconditions.checkArgument(creditsPerSec > 0, "Max write rate must be a positive number.");
    tagCreditsMap.put(RL_WRITE_TAG, creditsPerSec);
    return this;
  }

  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkNotNull(readFn, "TableReadFunction is required.");
    Preconditions.checkArgument(rateLimiter == null || tagCreditsMap.isEmpty(),
        "Only one of rateLimiter instance or read/write limits can be specified");
  }
}
