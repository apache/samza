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

package org.apache.samza.table.batching;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.utils.TableMetricsUtil;


/**
 * Wrapper of batch-related metrics.
 */
class BatchMetrics {
  /**
   * The number of batches
   */
  final Counter batchCount;

  /**
   * The time duration between opening and closing the batch
   */
  final Timer batchDuration;

  public BatchMetrics(TableMetricsUtil metricsUtil) {
    batchCount = metricsUtil.newCounter("batch-count");
    batchDuration = metricsUtil.newTimer("batch-duration");
  }

  public void incBatchCount() {
    batchCount.inc();
  }

  public void updateBatchDuration(long d) {
    batchDuration.update(d);
  }
}
