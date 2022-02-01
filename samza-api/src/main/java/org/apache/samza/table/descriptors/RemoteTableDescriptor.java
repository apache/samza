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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.table.batching.BatchProvider;
import org.apache.samza.table.remote.TablePart;
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
 * @param <U> the type of the update
 */
public class RemoteTableDescriptor<K, V, U> extends BaseTableDescriptor<K, V, RemoteTableDescriptor<K, V, U>> {

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
  //Key name for table api read rate limit
  public static final String READ_CREDITS = "io.read.credits";
  //Key name for table api write rate limit
  public static final String WRITE_CREDITS = "io.write.credits";
  public static final String READ_CREDIT_FN = "io.read.credit.func";
  public static final String WRITE_CREDIT_FN = "io.write.credit.func";
  public static final String ASYNC_CALLBACK_POOL_SIZE = "io.async.callback.pool.size";
  public static final String READ_RETRY_POLICY = "io.read.retry.policy";
  public static final String WRITE_RETRY_POLICY = "io.write.retry.policy";
  public static final String BATCH_PROVIDER = "io.batch.provider";

  // Input support for a specific remote store (optional)
  private TableReadFunction<K, V> readFn;

  // Output support for a specific remote store (optional)
  private TableWriteFunction<K, V, U> writeFn;

  // Rate limiter for client-side throttling; it is set by withRateLimiter()
  private RateLimiter rateLimiter;

  // Indicate whether read rate limiter is enabled or not
  private boolean enableReadRateLimiter = true;

  // Indicate whether write rate limiter is enabled or not
  private boolean enableWriteRateLimiter = true;

  // Batching support to reduce traffic volume sent to the remote store.
  private BatchProvider<K, V, U> batchProvider;

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
  public RemoteTableDescriptor<K, V, U> withReadFunction(TableReadFunction<K, V> readFn) {
    Preconditions.checkNotNull(readFn, "null read function");
    this.readFn = readFn;
    return this;
  }

  /**
   * Use specified TableWriteFunction with remote table and a retry policy.
   * @param writeFn write function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V, U> withWriteFunction(TableWriteFunction<K, V, U> writeFn) {
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    return this;
  }

  /**
   * Use specified {@link TableRetryPolicy} with the {@link TableReadFunction}.
   * @param retryPolicy retry policy for the write function
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V, U> withReadRetryPolicy(TableRetryPolicy retryPolicy) {
    Preconditions.checkNotNull(readFn, "null read function");
    Preconditions.checkNotNull(retryPolicy, "null retry policy");
    this.readRetryPolicy = retryPolicy;
    return this;
  }

  /**
   * Use specified {@link TableRetryPolicy} with the {@link TableWriteFunction}.
   * @param retryPolicy retry policy for the write function
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V, U> withWriteRetryPolicy(TableRetryPolicy retryPolicy) {
    Preconditions.checkNotNull(writeFn, "null write function");
    Preconditions.checkNotNull(retryPolicy, "null retry policy");
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
  public RemoteTableDescriptor<K, V, U> withRateLimiter(RateLimiter rateLimiter,
      TableRateLimiter.CreditFunction<K, V> readCreditFn,
      TableRateLimiter.CreditFunction<K, V> writeCreditFn) {
    Preconditions.checkNotNull(rateLimiter, "null read rate limiter");
    this.rateLimiter = rateLimiter;
    this.readCreditFn = readCreditFn;
    this.writeCreditFn = writeCreditFn;
    return this;
  }

  /**
   * Disable both read and write rate limiter. If the read rate limiter is enabled, the user must provide a rate limiter
   * by calling {@link #withRateLimiter(RateLimiter, TableRateLimiter.CreditFunction, TableRateLimiter.CreditFunction)}
   * or {@link #withReadRateLimit(int)}. If the write rate limiter is enabled,
   * the user must provide a rate limiter by calling {@link #withRateLimiter(RateLimiter, TableRateLimiter.CreditFunction,
   * TableRateLimiter.CreditFunction)} or {@link #withWriteRateLimit(int)}.
   * By default, both read and write rate limiters are enabled.
   *
   * @return this table descriptor instance.
   */
  public RemoteTableDescriptor<K, V, U> withRateLimiterDisabled() {
    withReadRateLimiterDisabled();
    withWriteRateLimiterDisabled();
    return this;
  }

  /**
   * Disable the read rate limiter.
   *
   * @return this table descriptor instance.
   */
  public RemoteTableDescriptor<K, V, U> withReadRateLimiterDisabled() {
    this.enableReadRateLimiter = false;
    return this;
  }

  /**
   * Disable the write rate limiter.
   *
   * @return this table descriptor instance.
   */
  public RemoteTableDescriptor<K, V, U> withWriteRateLimiterDisabled() {
    this.enableWriteRateLimiter = false;
    return this;
  }

  /**
   * Specify the rate limit for table read operations. If the read rate limit is set with this method
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter,
   * TableRateLimiter.CreditFunction, TableRateLimiter.CreditFunction)}
   * and vice versa.
   * Note that this is the total credit of rate limit for the entire job, each task will get a per task
   * credit of creditsPerSec/tasksCount. Hence creditsPerSec should be greater than total number of tasks.
   * @param creditsPerSec rate limit for read operations; must be positive and greater than total number tasks
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V, U> withReadRateLimit(int creditsPerSec) {
    Preconditions.checkArgument(creditsPerSec > 0, "Max read rate must be a positive number.");
    tagCreditsMap.put(RL_READ_TAG, creditsPerSec);
    return this;
  }

  /**
   * Specify the rate limit for table write operations. If the write rate limit is set with this method
   * it is invalid to call {@link RemoteTableDescriptor#withRateLimiter(RateLimiter,
   * TableRateLimiter.CreditFunction, TableRateLimiter.CreditFunction)}
   * and vice versa.
   * Note that this is the total credit of rate limit for the entire job, each task will get a per task
   * credit of creditsPerSec/tasksCount. Hence creditsPerSec should be greater than total number of tasks.
   * @param creditsPerSec rate limit for write operations; must be positive and greater than total number tasks
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V, U> withWriteRateLimit(int creditsPerSec) {
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
  public RemoteTableDescriptor<K, V, U> withAsyncCallbackExecutorPoolSize(int poolSize) {
    this.asyncCallbackPoolSize = poolSize;
    return this;
  }

  /**
   * Specifies a batch provider inorder to batch Table operations.
   * */
  public RemoteTableDescriptor<K, V, U> withBatchProvider(BatchProvider<K, V, U> batchProvider) {
    this.batchProvider = batchProvider;
    return this;
  }

  @Override
  public String getProviderFactoryClassName() {
    return PROVIDER_FACTORY_CLASS_NAME;
  }

  @Override
  public Map<String, String> toConfig(Config jobConfig) {

    Map<String, String> tableConfig = new HashMap<>(super.toConfig(jobConfig));

    writeRateLimiterConfig(jobConfig, tableConfig);

    // Handle readCredit functions
    if (readCreditFn != null) {
      addTableConfig(READ_CREDIT_FN, SerdeUtils.serialize("read credit function", readCreditFn), tableConfig);
      addTablePartConfig(READ_CREDIT_FN, readCreditFn, jobConfig, tableConfig);
    }

    // Handle writeCredit functions
    if (writeCreditFn != null) {
      addTableConfig(WRITE_CREDIT_FN, SerdeUtils.serialize("write credit function", writeCreditFn), tableConfig);
      addTablePartConfig(WRITE_CREDIT_FN, writeCreditFn, jobConfig, tableConfig);
    }

    // Handle read retry policy
    if (readRetryPolicy != null) {
      addTableConfig(READ_RETRY_POLICY, SerdeUtils.serialize("read retry policy", readRetryPolicy), tableConfig);
      addTablePartConfig(READ_RETRY_POLICY, readRetryPolicy, jobConfig, tableConfig);
    }

    // Handle write retry policy
    if (writeRetryPolicy != null) {
      addTableConfig(WRITE_RETRY_POLICY, SerdeUtils.serialize("write retry policy", writeRetryPolicy), tableConfig);
      addTablePartConfig(WRITE_RETRY_POLICY, writeRetryPolicy, jobConfig, tableConfig);
    }

    addTableConfig(ASYNC_CALLBACK_POOL_SIZE, String.valueOf(asyncCallbackPoolSize), tableConfig);

    // Handle table reader function
    if (readFn != null) {
      addTableConfig(READ_FN, SerdeUtils.serialize("read function", readFn), tableConfig);
      addTablePartConfig(READ_FN, readFn, jobConfig, tableConfig);
    }

    // Handle table write function
    if (writeFn != null) {
      addTableConfig(WRITE_FN, SerdeUtils.serialize("write function", writeFn), tableConfig);
      addTablePartConfig(WRITE_FN, writeFn, jobConfig, tableConfig);
    }

    if (batchProvider != null) {
      addTableConfig(BATCH_PROVIDER, SerdeUtils.serialize("batch provider", batchProvider), tableConfig);
      addTablePartConfig(BATCH_PROVIDER, batchProvider, jobConfig, tableConfig);
    }
    return Collections.unmodifiableMap(tableConfig);
  }

  // Handle rate limiter
  private void writeRateLimiterConfig(Config jobConfig, Map<String, String> tableConfig) {
    if (!tagCreditsMap.isEmpty()) {
      RateLimiter defaultRateLimiter;
      try {
        @SuppressWarnings("unchecked")
        Class<? extends RateLimiter> clazz = (Class<? extends RateLimiter>) Class.forName(DEFAULT_RATE_LIMITER_CLASS_NAME);
        Constructor<? extends RateLimiter> ctor = clazz.getConstructor(Map.class);
        defaultRateLimiter = ctor.newInstance(tagCreditsMap);
      } catch (Exception ex) {
        throw new SamzaException("Failed to create default rate limiter", ex);
      }
      addTableConfig(RATE_LIMITER, SerdeUtils.serialize("rate limiter", defaultRateLimiter), tableConfig);
      if (defaultRateLimiter instanceof TablePart) {
        addTablePartConfig(RATE_LIMITER, (TablePart) defaultRateLimiter, jobConfig, tableConfig);
      }
    } else if (rateLimiter != null) {
      addTableConfig(RATE_LIMITER, SerdeUtils.serialize("rate limiter", rateLimiter), tableConfig);
      if (rateLimiter instanceof TablePart) {
        addTablePartConfig(RATE_LIMITER, (TablePart) rateLimiter, jobConfig, tableConfig);
      }
    }
    //Write table api read/write rate limit
    if (this.enableReadRateLimiter && tagCreditsMap.containsKey(RL_READ_TAG)) {
      addTableConfig(READ_CREDITS, String.valueOf(tagCreditsMap.get(RL_READ_TAG)), tableConfig);
    }
    if (this.enableWriteRateLimiter && tagCreditsMap.containsKey(RL_WRITE_TAG)) {
      addTableConfig(WRITE_CREDITS, String.valueOf(tagCreditsMap.get(RL_WRITE_TAG)), tableConfig);
    }
  }

  @Override
  protected void validate() {
    Preconditions.checkArgument(writeFn != null || readFn != null,
        "Must have one of TableReadFunction or TableWriteFunction");
    Preconditions.checkArgument(rateLimiter == null || tagCreditsMap.isEmpty(),
        "Only one of rateLimiter instance or read/write limits can be specified");
    // Assume callback executor pool should have no more than 20 threads
    Preconditions.checkArgument(asyncCallbackPoolSize <= 20,
        "too many threads for async callback executor.");

    if (readFn != null && enableReadRateLimiter) {
      Preconditions.checkArgument(readCreditFn != null || tagCreditsMap.containsKey(RL_READ_TAG),
          "Read rate limiter is enabled, there is no read rate limiter configured.");
    }

    if (writeFn != null && enableWriteRateLimiter) {
      Preconditions.checkArgument(writeCreditFn != null || tagCreditsMap.containsKey(RL_WRITE_TAG),
          "Write rate limiter is enabled, there is no write rate limiter configured.");
    }
  }

  /**
   * Helper method to add table part config items to table configuration
   * @param tablePartKey key of the table part
   * @param tablePart table part
   * @param jobConfig job configuration
   * @param tableConfig table configuration
   */
  protected void addTablePartConfig(String tablePartKey, TablePart tablePart, Config jobConfig,
      Map<String, String> tableConfig) {
    tablePart.toConfig(jobConfig, new MapConfig(tableConfig))
        .forEach((k, v) -> addTableConfig(String.format("%s.%s", tablePartKey, k), v, tableConfig));
  }
}
