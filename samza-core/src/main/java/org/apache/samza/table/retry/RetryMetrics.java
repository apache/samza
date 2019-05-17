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

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.utils.TableMetricsUtil;


/**
 * Retry-related metrics
 */
class RetryMetrics {
  /**
   * Number of retries executed (excluding the first attempt)
   */
  final Counter retryCount;

  /**
   * Number of successes with only the first attempt
   */
  final Counter successCount;

  /**
   * Number of operations that failed permanently and exhausted all retries
   */
  final Counter permFailureCount;

  /**
   * Total time spent in each IO; this is updated only
   * when at least one retries have been attempted.
   */
  final Timer retryTimer;

  public RetryMetrics(String prefix, TableMetricsUtil metricsUtil) {
    retryCount = metricsUtil.newCounter(prefix + "-retry-count");
    successCount = metricsUtil.newCounter(prefix + "-success-count");
    permFailureCount = metricsUtil.newCounter(prefix + "-perm-failure-count");
    retryTimer = metricsUtil.newTimer(prefix + "-retry-timer");
  }
}
