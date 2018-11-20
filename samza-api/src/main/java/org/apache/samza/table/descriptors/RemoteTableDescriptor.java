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

package org.apache.samza.table.descriptors;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.retry.TableRetryPolicy;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.util.RateLimiter;

import com.google.common.base.Preconditions;

/**
 * Table descriptor for remote store backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RemoteTableDescriptor<K, V> extends BaseTableDescriptor<K, V, RemoteTableDescriptor<K, V>> {

  public static final String PROVIDER_FACTORY_CLASS_NAME = "org.apache.samza.table.remote.RemoteTableProviderFactory";

  public static final String DEFAULT_RATE_LIMITER_CLASS_NAME = "org.apache.samza.util.EmbeddedTaggedRateLimiter";

  /**
   * Tag to be used for provision credits for rate limiting read operations from the remote table.
   * Caller must pre-populate the credits with this tag when specifying a custom rate limiter instance
   * through {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, TableRateLimiter.CreditFunction,
   * TableRateLimiter.CreditFunction)}
   */
  public static final String RL_READ_TAG = "readTag";

  /**
   * Tag to be used for provision credits for rate limiting write operations into the remote table.
   * Caller can optionally populate the credits with this tag when specifying a custom rate limiter instance
   * through {@link RemoteTableDescriptor#withRateLimiter(RateLimiter, TableRateLimiter.CreditFunction,
   * TableRateLimiter.CreditFunction)} and it needs the write functionality.
   */
  public static final String RL_WRITE_TAG = "writeTag";

  public static final String READ_FN = "io.read.func";
  public static final String WRITE_FN = "io.write.func";
  public static final String RATE_LIMITER = "io.ratelimiter";
  public static final String READ_CREDIT_FN = "io.read.credit.func";
  public static final String WRITE_CREDIT_FN = "io.write.credit.func";
  public static final String ASYNC_CALLBACK_POOL_SIZE = "io.async.callback.pool.size";
  public static final String READ_RETRY_POLICY = "io.read.retry.policy";
  public static final String WRITE_RETRY_POLICY = "io.write.retry.policy";

  // Input support for a specific remote store (required)
  private TableReadFunction<K, V> readFn;

  // Output support for a specific remote store (optional)
  private TableWriteFunction<K, V> writeFn;

  // Rate limiter for client-side throttling; it is set by withRateLimiter()
  private RateLimiter rateLimiter;

  // Rates for constructing the default rate limiter when they are non-zero
  private Map<String, Integer> tagCreditsMap = new HashMap<>();

  private TableRateLimiter.CreditFunction<K, V> readCreditFn;
  private TableRateLimiter.CreditFunction<K, V> writeCreditFn;

  private TableRetryPolicy readRetryPolicy;
  private TableRetryPolicy writeRetryPolicy;

  // By default execute future callbacks on the native client threads
  // ie. no additional thread pool for callbacks.
  private int asyncCallbackPoolSize = -1;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   */
  public RemoteTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Use specified TableReadFunction with remote table and a retry policy.
   * @param readFn read function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withReadFunction(TableReadFunction<K, V> readFn) {
    Preconditions.checkNotNull(readFn, "null read function");
    this.readFn = readFn;
    return this;
  }

  /**
   * Use specified TableWriteFunction with remote table and a retry policy.
   * @param writeFn write function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteFunction(TableWriteFunction<K, V> writeFn) {
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    return this;
  }

  /**
   * Use specified TableReadFunction with remote table.
   * @param readFn read function instance
   * @param retryPolicy retry policy for the read function
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withReadFunction(TableReadFunction<K, V> readFn, TableRetryPolicy retryPolicy) {
    Preconditions.checkNotNull(readFn, "null read function");
    Preconditions.checkNotNull(retryPolicy, "null retry policy");
    this.readFn = readFn;
    this.readRetryPolicy = retryPolicy;
    return this;
  }

  /**
   * Use specified TableWriteFunction with remote table.
   * @param writeFn write function instance
   * @param retryPolicy retry policy for the write function
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteFunction(TableWriteFunction<K, V> writeFn, TableRetryPolicy retryPolicy) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(retryPolicy, "null retry policy");
    this.writeFn = writeFn;
    this.writeRetryPolicy = retryPolicy;
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
  public RemoteTableDescriptor<K, V> withRateLimiter(RateLimiter rateLimiter,
      TableRateLimiter.CreditFunction<K, V> readCreditFn,
      TableRateLimiter.CreditFunction<K, V> writeCreditFn) {
    Preconditions.checkNotNull(rateLimiter, "null read rate limiter");
    this.rateLimiter = rateLimiter;
    this.readCreditFn = readCreditFn;
    this.writeCreditFn = writeCreditFn;
    return this;
  }

  /**
   * Specify the rate limit for table read operations. If the read rate limit is set with this method
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter,
   * TableRateLimiter.CreditFunction, TableRateLimiter.CreditFunction)}
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
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter,
   * TableRateLimiter.CreditFunction, TableRateLimiter.CreditFunction)}
   * and vice versa.
   * @param creditsPerSec rate limit for write operations; must be positive
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteRateLimit(int creditsPerSec) {
    Preconditions.checkArgument(creditsPerSec > 0, "Max write rate must be a positive number.");
    tagCreditsMap.put(RL_WRITE_TAG, creditsPerSec);
    return this;
  }

  /**
   * Specify the size of the thread pool for the executor used to execute
   * callbacks of CompletableFutures of async Table operations. By default, these
   * futures are completed (called) by the threads of the native store client. Depending
   * on the implementation of the native client, it may or may not allow executing long
   * running operations in the callbacks. This config can be used to execute the callbacks
   * from a separate executor to decouple from the native client. If configured, this
   * thread pool is shared by all read and write operations.
   * @param poolSize max number of threads in the executor for async callbacks
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withAsyncCallbackExecutorPoolSize(int poolSize) {
    this.asyncCallbackPoolSize = poolSize;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getProviderFactoryClassName() {
    return PROVIDER_FACTORY_CLASS_NAME;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void generateConfig(Config jobConfig, Map<String, String> tableConfig) {

    super.generateConfig(jobConfig, tableConfig);

    // Serialize and store reader/writer functions
    addTableConfig(READ_FN, SerdeUtils.serialize("read function", readFn), tableConfig);

    if (writeFn != null) {
      addTableConfig(WRITE_FN, SerdeUtils.serialize("write function", writeFn), tableConfig);
    }

    if (!tagCreditsMap.isEmpty()) {
      RateLimiter defaultRateLimiter;
      try {
        Class<? extends RateLimiter> clazz = (Class<? extends RateLimiter>) Class.forName(DEFAULT_RATE_LIMITER_CLASS_NAME);
        Constructor<? extends RateLimiter> ctor = clazz.getConstructor(Map.class);
        defaultRateLimiter = ctor.newInstance(tagCreditsMap);
      } catch (Exception ex) {
        throw new SamzaException("Failed to create default rate limiter", ex);
      }
      addTableConfig(RATE_LIMITER, SerdeUtils.serialize("rate limiter", defaultRateLimiter), tableConfig);
    } else if (rateLimiter != null) {
      addTableConfig(RATE_LIMITER, SerdeUtils.serialize("rate limiter", rateLimiter), tableConfig);
    }

    // Serialize the readCredit functions
    if (readCreditFn != null) {
      addTableConfig(READ_CREDIT_FN, SerdeUtils.serialize("read credit function", readCreditFn), tableConfig);
    }
    // Serialize the writeCredit functions
    if (writeCreditFn != null) {
      addTableConfig(WRITE_CREDIT_FN, SerdeUtils.serialize("write credit function", writeCreditFn), tableConfig);
    }

    if (readRetryPolicy != null) {
      addTableConfig(READ_RETRY_POLICY, SerdeUtils.serialize("read retry policy", readRetryPolicy), tableConfig);
    }

    if (writeRetryPolicy != null) {
      addTableConfig(WRITE_RETRY_POLICY, SerdeUtils.serialize("write retry policy", writeRetryPolicy), tableConfig);
    }

    addTableConfig(ASYNC_CALLBACK_POOL_SIZE, String.valueOf(asyncCallbackPoolSize), tableConfig);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkNotNull(readFn, "TableReadFunction is required.");
    Preconditions.checkArgument(rateLimiter == null || tagCreditsMap.isEmpty(),
        "Only one of rateLimiter instance or read/write limits can be specified");
    // Assume callback executor pool should have no more than 20 threads
    Preconditions.checkArgument(asyncCallbackPoolSize <= 20,
        "too many threads for async callback executor.");
  }
}
