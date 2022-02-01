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

import java.io.Serializable;
import java.time.Duration;
import org.apache.samza.table.remote.TablePart;


public abstract class BatchProvider<K, V, U> implements TablePart, Serializable {
  public abstract Batch<K, V, U> getBatch();

  private int maxBatchSize = 100;
  private Duration maxBatchDelay = Duration.ofMillis(100);

  public BatchProvider<K, V, U> withMaxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public BatchProvider<K, V, U> withMaxBatchDelay(Duration maxBatchDelay) {
    this.maxBatchDelay = maxBatchDelay;
    return this;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public Duration getMaxBatchDelay() {
    return maxBatchDelay;
  }
}
